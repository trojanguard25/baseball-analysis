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

class GetPlayerComparisons(luigi.Task):
    year = luigi.Parameter()
    pos = luigi.Parameter()
    top = luigi.Parameter()
    
    def requires(self):
        return GetPlayersRanked(self.year, self.pos, self.top)

    def run(self):
        players = {}
        with self.output().open('w') as out_f:
            out_writer = csv.writer(out_f)
            header = ['players', 'better']
            out_writer.writerow(header)
            with self.input().open('r') as in_f:
                in_csv = csv.DictReader(in_f)
                for row in in_csv:
                    new_uuid = row['key_person']
                    new_war = float(row['total_war'])
                    
                    for uuid, war in players.iteritems():
                        players_compared = '{0}_{1}'.format(new_uuid, uuid)
                        better = 0
                        if new_war > war:
                            better = 1
                        elif war > new_war:
                            better = -1

                        out_writer.writerow([players_compared, str(better)])

                    players[new_uuid] = new_war

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/{0}/player_comparisons_{1}_top_{2}.csv'.format(self.year, self.pos, self.top))


class CalcPlayerResults(luigi.Task):
    year = luigi.Parameter()
    pos = luigi.Parameter()
    top = luigi.Parameter()
    
    def requires(self):
        requirements = {'players': GetPlayersRanked(self.year, self.pos, self.top)}
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        find_sql = 'select l.list_id from lists l '
        find_sql += ' join rankings r on r.list_id = l.list_id '
        find_sql += ' where r.rank = {0} and l.year = {1}'.format(str(self.top), str(self.year))
        cur = conn.cursor()
        cur.execute(find_sql)

        requirements['lists'] = {result[0] : GetListResults(result[0]) for result in cur}

        return requirements

    def run(self):
        with self.output().open('w') as out_f:
            out_writer = csv.writer(out_f)
            header = ['players', 'better']
            rankings = {}
            for list_id, t in self.input()['lists'].iteritems():
                header.append(list_id)
                ranking = {}
                with t.open('r') as in_f:
                    mycsv = csv.DictReader(in_f)
                    for row in mycsv:
                        uuid = row['key_person']
                        rank = int(row['rank'])
                        ranking[uuid] = rank

                rankings[list_id] = ranking
        

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/{0}/player_comparisons_with_results_{1}_top_{2}.csv'.format(self.year, self.pos, self.top))


class GetPlayersRanked(luigi.Task):
    year = luigi.Parameter()
    pos = luigi.Parameter()
    top = luigi.Parameter()
    
    def requires(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        find_sql = 'select l.list_id from lists l '
        find_sql += ' join rankings r on r.list_id = l.list_id '
        find_sql += ' where r.rank = {0} and l.year = {1}'.format(str(self.top), str(self.year))
        cur = conn.cursor()
        cur.execute(find_sql)

        return {result[0] : GetListResults(result[0]) for result in cur}

    def run(self):
        br = BRSource()
        with self.output().open('w') as out_f:
            players = {}
            for list_id, t in self.input().iteritems():
                with t.open('r') as in_f:
                    mycsv = csv.DictReader(in_f)
                    rank = 0
                    for row in mycsv:
                        rank = int(row['rank'])
                        if rank > int(self.top):
                            break

                        uuid = row['key_person']
                        if uuid in players:
                            continue
                        if self.pos == 'p':
                            if not br.isPitcher(uuid):
                                continue
                        if self.pos == 'h':
                            if not br.isHitter(uuid):
                                continue

                        players[uuid] = [row['key_person'], row['name'], row['total_war']]

            csv_writer = csv.writer(out_f)
            header = ['key_person', 'name', 'total_war']
            csv_writer.writerow(header)
            for uuid, info in players.iteritems():
                csv_writer.writerow(info)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/prospects/{0}/players_ranked_{1}_top_{2}.csv'.format(self.year, self.pos, self.top))

class GetListResults(luigi.Task):
    list_id = luigi.Parameter()

    def run(self):
        br = BRSource()
        with self.output().open('wb') as out_f:
            csv_writer = csv.writer(out_f)

            header = ['key_person', 'rank', 'name', 'total_war']
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


