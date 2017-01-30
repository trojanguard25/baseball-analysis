import luigi
import pymysql
import csv
import re

import sys
import math
import time


sys.path.append('/home/dan/github/player-finder/src/')
from baseballReferenceSource import BRSource
from config import Config

class GetListResults(luigi.Task):
    list_id = luigi.Parameter()

    def run(self):
        br = BRSource()
        with self.output().open('wb') as out_f:
            csv_writer = csv.writer(out_f)

            header = ['key_person', 'name', 'rank', 'total_war']
            csv_writer.writerow(header)
            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

            find_sql = 'SELECT key_person, rank FROM rankings WHERE list_id={0} order by rank'.format(str(self.list_id))
            cur = conn.cursor()
            cur.execute(find_sql)

            for result in cur:
                line = []
                uuid = result[0]
                line.append(result[0])
                line.append(result[1])

                line.append(br.getName(uuid))

                total_war = br.getTeamControlWar(uuid).total_war
                line.append(str(total_war))

                csv_writer.writerow(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/list_results/list_{0}.csv'.format(self.list_id))

class CalcListPerformance(luigi.Task):
    list_id = luigi.Parameter()
    top = luigi.Parameter()

    def requires(self):
        return GetListResults(self.list_id)

    def run(self):
        with self.output().open('w') as out_f:
            with self.input().open() as in_f:
                mycsv = csv.DictReader(in_f)
                total_war = 0.0
                results = []
                count = 0
                for row in mycsv:
                    results.append(float(row['total_war']))
                    total_war += float(row['total_war'])
                    count += 1
                    if count == self.top:
                        break

                total_comparisons = len(results) * (len(results) - 1) / 2;
                num_correct = 0
                for x in range(0, len(results)):
                    for y in range(x+1, len(results)):
                        if results[x] > results[y]:
                            num_correct += 1

                out_f.write("total_war = {0}\n".format(total_war))
                out_f.write("num_correcct = {0}\n".format(num_correct))
                

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/list_results/performance_{0}_top_{1}.txt'.format(self.list_id, self.top))

class CalcYearResults(luigi.Task):
    year = luigi.Parameter()
    top = luigi.Parameter()

    def requires(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        find_sql = 'select l.list_id from lists l '
        find_sql += ' join rankings r on r.list_id = l.list_id '
        find_sql += ' where r.rank = {0} and l.year = {1}'.format(str(self.top), str(self.year))
        cur = conn.cursor()
        cur.execute(find_sql)

        return {result[0] : CalcListPerformance(result[0], self.top) for result in cur}

    def run(self):
        with self.output().open('w') as out_f:
            for list_id, t in self.input().iteritems():
                with t.open('r') as in_f:
                    for line in in_f:
                        out_f.write(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/{0}/results_top_{1}.txt'.format(str(self.year), str(self.top)))


