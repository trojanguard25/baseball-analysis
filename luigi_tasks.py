import luigi
import pymysql

import sys

sys.path.append('/home/dan/github/player-finder/src/')
from baseballReferenceSource import BRSource
from config import Config

class WarSeasons(luigi.Task):
    uuid = luigi.Parameter()

    def run(self):
        br = BRSource()
        wardict = br.getBatWar(self.uuid)

        with self.output().open('w') as out_file:
            for key in sorted(wardict):
                out_file.write(key + "," + str(wardict[key]) + '\n')

    def output(self):
        return luigi.LocalTarget('/home/dan/data/total_war_{0}.csv'.format(str(self.uuid)))

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
