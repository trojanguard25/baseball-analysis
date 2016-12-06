import luigi
import pymysql
import csv

import sys
import math

sys.path.append('/home/dan/github/player-finder/src/')
from baseballReferenceSource import BRSource
from config import Config

class AgeSeasons(luigi.Task):
    uuid = luigi.Parameter()

    def run(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        sql = 'select birth_year, birth_month, birth_day, pro_played_first, pro_played_last'
        sql += ' from chadwickbureau '
        sql += ' where key_person = \'' + str(self.uuid) + '\''

        cur = conn.cursor()
        cur.execute(sql)

        year = 0
        month = 0
        day = 0
        first = 0
        last = 0
        for row in cur:
            year = int(row[0])
            month = int(row[1])
            day = int(row[2])
            first = int(row[3])
            last = int(row[4])
           
        with self.output().open('w') as out_file:
            for yr in range(first, last+1):
                age = yr - year - 1
                if month <= 6:
                    age += 1

                out_file.write(str(yr) + "," + str(age) + '\n')


    def output(self):
        return luigi.LocalTarget('/home/dan/data/age/age_seasons_{0}.csv'.format(str(self.uuid)))

class BatWarSeasons(luigi.Task):
    uuid = luigi.Parameter()

    def run(self):
        br = BRSource()
        war = br.getBatWar(self.uuid)

        with self.output().open('w') as out_file:
            for year in war.years:
                bat_war = war._year_wars[year].bat_war
                out_file.write(str(year) + "," + str(bat_war) + '\n')

    def output(self):
        return luigi.LocalTarget('/home/dan/data/war/bat/by_year_{0}.csv'.format(str(self.uuid)))

class BatWarcel(luigi.Task):
    uuid = luigi.Parameter()
    year = luigi.Parameter()

    def requires(self):
        return {'age' : AgeSeasons(self.uuid), 
                'war' : BatWarSeasons(self.uuid)}

    def run(self):
        age = 0
        year = 0
        with self.input()['age'].open() as age_f:
            reader = csv.reader(age_f)
            for row in reader:
                age = int(row[1])
                year = int(row[0])
                break

        war = {}
        with self.input()['war'].open() as war_f:
            reader = csv.reader(war_f)
            for row in reader:
                if row[0] != 'Total':
                    war[int(row[0])] = float(row[1])

        p_age = age + (int(self.year) - year)

        warcel = []
        w_1 = 0
        if int(self.year) - 1 in war:
            w_1 = war[int(self.year) - 1]
        warcel.insert(0, w_1)
        w_2 = 0
        if int(self.year) - 2 in war:
            w_2 = war[int(self.year) - 2]
        warcel.insert(0, w_2)
        w_3 = 0
        if int(self.year) - 3 in war:
            w_3 = war[int(self.year) - 3]
        warcel.insert(0, w_3)

        w_a = 0.1 * w_3 + 0.3 * w_2 + 0.6 * w_1

        w_b = 0.8 * w_a

        w0 = w_b + ((30 - p_age) * 0.1)
        warcel.append(w0)
        w1 = w0 - 0.4 + ((30 - p_age) * 0.08)
        warcel.append(w1)
        w2 = w1 - 0.4 + ((30 - p_age) * 0.03)
        warcel.append(w2)
        w3 = w2 - 0.4 + ((30 - p_age) * 0.03)
        warcel.append(w3)
        w4 = w3 - 0.4 + ((30 - p_age) * 0.03)
        warcel.append(w4)

        with self.output().open('w') as out_file:
            year = int(self.year) - 3
            for war in warcel:
                out_file.write(str(year) + "," + str(war) + '\n')
                year += 1

    def output(self):
        return luigi.LocalTarget('/home/dan/data/warcel/bat/{0}/{1}.csv'.format(str(self.year), str(self.uuid)))

class WarcelError(luigi.Task):
    uuid = luigi.Parameter()
    year = luigi.Parameter()
    
    def requires(self):
        return {'warcel' : BatWarcel(self.uuid, self.year), 
                'war' : BatWarSeasons(self.uuid)}

    def run(self):
        war = {}
        with self.input()['war'].open() as war_f:
            reader = csv.reader(war_f)
            for row in reader:
                if row[0] != 'Total':
                    war[int(row[0])] = float(row[1])

        errors = []
        with self.input()['warcel'].open() as warcel_f:
            reader = csv.reader(warcel_f)
            for row in reader:
                year = int(row[0])
                warcel_war = float(row[1])
                if year in war:
                    errors.append([str(year),str(warcel_war), str(war[year]),str(warcel_war - war[year])])
                else:
                    errors.append([str(year),str(warcel_war), "0",str(warcel_war)])

        with self.output().open('w') as out_file:
            year = int(self.year)
            for err in errors:
                out_file.write(",".join(err) + '\n')

    def output(self):
        return luigi.LocalTarget('/home/dan/data/warcel/bat/{0}/{1}_error.csv'.format(str(self.year), str(self.uuid)))

class TestTask(luigi.Task):
    def requires(self):
        #return AgeSeasons('f322d40f')
        return WarcelError('f322d40f', 2013)
        #return WarcelError('4b6d5f1d', 2017)

    def run(self):
        print "hi"

    def output(self):
        return luigi.LocalTarget('home/dan/data/test.txt')

class WarAverageError(luigi.Task):
    year = luigi.IntParameter()

    def requires(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        sql = 'select c.key_person'
        sql += ' from chadwickbureau c '
        sql += ' join br_war_bat b '
        sql += ' on c.key_bbref = b.player_ID '
        sql += ' where c.mlb_played_first <= {0} and c.mlb_played_last > {1} and b.pitcher = \'N\' '.format(str(self.year-3), str(self.year-1)) 
        sql += ' group by c.key_person '

        cur = conn.cursor()
        cur.execute(sql)

        return [WarcelError(str(uuid[0]),self.year) for uuid in cur]

    def run(self):
        total = 0
        errors = [0.0, 0.0, 0.0, 0.0, 0.0]
        for t in self.input():
            total += 1
            with t.open('r') as in_file:
                reader = csv.reader(in_file)
                i = 0
                for row in reader:
                    if int(row[0]) < self.year:
                        continue

                    errors[i] += math.fabs(float(row[3]))
                    i += 1


        with self.output().open('w') as out_file:
            out_file.write(str(total) + "\n")
            out_file.write(".".join(map(str,errors)))

    def output(self):
        return luigi.LocalTarget('/home/dan/data/warcel/bat/{0}/total_error.txt'.format(str(self.year)))


class WarList(luigi.Task):
    def requires(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        sql = 'select c.key_person'
        sql += ' from chadwickbureau c '
        sql += ' join br_war_bat b '
        sql += ' on c.key_bbref = b.player_ID '
        sql += ' where c.mlb_played_first <= 2009 and c.mlb_played_last > 2011 and b.pitcher = \'N\' ' 
        sql += ' group by c.key_person '

        cur = conn.cursor()
        cur.execute(sql)

        return [WarSeasons(str(uuid[0])) for uuid in cur]

    def run(self):
        with self.output().open('w') as out_file:
            for t in self.input():
                with t.open('r') as in_file:
                    for line in in_file:
                        out_file.write(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/total.txt')

if __name__ == "__main__":
    luigi.run()
