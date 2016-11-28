import luigi
import pymysql

import sys

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

class WarSeasons(luigi.Task):
    uuid = luigi.Parameter()

    def run(self):
        br = BRSource()
        war = br.getBatWar(self.uuid)

        with self.output().open('w') as out_file:
            for year in war.years:
                bat_war = war._year_wars[year].bat_war
                out_file.write(str(year) + "," + str(bat_war) + '\n')

    def output(self):
        return luigi.LocalTarget('/home/dan/data/war_by_year_{0}.csv'.format(str(self.uuid)))


class TestTask(luigi.Task):
    def requires(self):
        return AgeSeasons('f322d40f')

    def run(self):
        print "hi"

    def output(self):
        return luigi.LocalTarget('home/dan/data/test.txt')

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
