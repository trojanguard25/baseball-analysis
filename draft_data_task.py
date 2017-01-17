import luigi
import pymysql
import csv
import re

import sys
import math
import time

import urllib2
from bs4 import BeautifulSoup

sys.path.append('/home/dan/github/player-finder/src/')
from baseballReferenceSource import BRSource
from config import Config

url_template = 'http://www.baseball-reference.com/draft/index.cgi?year_ID={0}&draft_round={1}&draft_type=junreg&query_type=year_round'


team_map = {'Diamondbacks' : 'ARI',
            'D\'backs' : 'ARI',
            'Braves' : 'ATL',
            'Orioles' : 'BAL',
            'Red Sox' : 'BOS',
            'Cubs' : 'CHC',
            'White Sox' : 'CHW',
            'Reds' : 'CIN',
            'Indians' : 'CLE',
            'Rockies' : 'COL',
            'Tigers' : 'DET',
            'Astros' : 'HOU',
            'Royals' : 'KCR',
            'Angels' : 'LAA',
            'Dodgers' : 'LAD',
            'Marlins' : 'MIA',
            'Brewers' : 'MIL',
            'Twins' : 'MIN',
            'Mets' : 'NYM',
            'Yankees' : 'NYY',
            'Athletics' : 'OAK',
            'Phillies' : 'PHI',
            'Pirates' : 'PIT',
            'Padres' : 'SDP',
            'Mariners' : 'SEA',
            'Giants' : 'SFG',
            'Cardinals' : 'STL',
            'Rays' : 'TBR',
            'Devil Rays' : 'TBR',
            'Rangers' : 'TEX',
            'Blue Jays' : 'TOR',
            'Expos' : 'WSN',
            'Nationals': 'WSN'}

class CalcTeamResults(luigi.Task):
    start_year = luigi.Parameter()
    end_year = luigi.Parameter()

    def requires(self):
        teams = set()
        for x, team in team_map.items():
            teams.add(team)
        return [GetTeamPicks(team) for team in teams]

    def run(self):
        with self.output().open('w') as out_file:
            for t in self.input():
                total_war = 0.0
                total_expected_war = 0.0
                team = ''
                with t.open('r') as in_f:
                    mycsv = csv.DictReader(in_f)
                    for row in mycsv:
                        year = int(row['year'])
                        if year < int(self.start_year) or year > int(self.end_year):
                            continue
                        
                        team = row['team']
                        total_war += float(row['total_war'])

                        expected_war = float(row['expected_war'])
                        if 2016 - year < 3:
                            expected_war = 0.0
                        elif 2014 - year < 7:
                            expected_war *= float(2014 - year) / 7.0

                        total_expected_war += expected_war

                    out_file.write('{0},{1},{2},{3}\n'.format(team, str(total_war), str(total_expected_war), str(total_war - total_expected_war)))

    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/expected/results_{0}_{1}.csv'.format(self.start_year, self.end_year))

class GetPickValues(luigi.Task):
    def run(self):
        br = BRSource()
        with self.output().open('wb') as out_f:
            csv_writer = csv.writer(out_f)

            header = ['ovpick', 'team', 'type', 'pos', 'total_war']
            csv_writer.writerow(header)
            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

            find_sql = 'SELECT ovpick, team, key_person, type, pos FROM  draft where signed <>\'N\''
            cur = conn.cursor()
            cur.execute(find_sql)

            for result in cur:
                line = []
                line.append(result[0])
                line.append(result[1])
                uuid = result[2]
                line.append(result[3])
                line.append(result[4])

                total_war = br.getTeamControlWar(uuid).total_war
                line.append(str(total_war))

                csv_writer.writerow(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/team_control_results.csv')

class GetTeamPicks(luigi.Task):
    team = luigi.Parameter()

    def run(self):
        br = BRSource()
        with self.output().open('wb') as out_f:
            csv_writer = csv.writer(out_f)

            header = ['year', 'ovpick', 'team', 'key_person', 'signed', 'type', 'total_war', 'expected_war']
            csv_writer.writerow(header)
            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

            find_sql = 'SELECT year, ovpick, team, key_person, signed, type FROM  draft where team=\'{0}\''.format(str(self.team))
            cur = conn.cursor()
            cur.execute(find_sql)

            for result in cur:
                line = []
                year = int(result[0])
                line.append(result[0])
                ovpick = float(result[1])
                line.append(result[1])
                line.append(result[2])
                uuid = result[3]
                line.append(uuid)
                signed = result[4]
                line.append(signed)
                line.append(result[5])

                total_war = br.getTeamControlWar(uuid).total_war
                if not signed == 'N':
                    line.append(str(total_war))
                else:
                    line.append('0')

                expect_war = 17.41 * (ovpick ** (-0.68))
                line.append(str(expect_war))
                
                csv_writer.writerow(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/expected/{0}_by_pick.csv'.format(self.team))
                        


class InputYears(luigi.Task):
    start_year = luigi.Parameter()
    end_year = luigi.Parameter()

    def requires(self):
        return [InputDraftYear(year) for year in range(int(self.start_year),int(self.end_year)+1)]


class InputDraftYear(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return [InputYearRound(self.year, rnd) for rnd in range(1,11)]

    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/{0}/year_done'.format(self.year))

class InputYearRound(luigi.Task):
    year = luigi.Parameter()
    rnd = luigi.Parameter()

    def requires(self):
        return ScrapeYearRound(self.year, self.rnd)

    def run(self):
        with self.output().open('w') as out_f:
            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

            delete_sql = 'DELETE FROM draft where year={0} and rnd={1}'.format(str(self.year), str(self.rnd))
            out_f.write(delete_sql + "\n")
            cur = conn.cursor()
            cur.execute(delete_sql)

            via_re = re.compile('^(.+) via ')
            rnd_re = re.compile('^(\d+)\D')

            with self.input().open('r') as in_f:
                mycsv = csv.DictReader(in_f)
                for row in mycsv:
                    year = row['Year']
                    full_rnd = row['Rnd']
                    m = rnd_re.search(full_rnd)
                    if m:
                        full_rnd = m.group(1)
                    rnd = full_rnd
                    ovpck = row['OvPck']
                    full_tm = row['Tm']
                    m = via_re.search(full_tm)
                    if m:
                        full_tm = m.group(1)
                    tm = team_map[full_tm]
                    signed = row['Signed']
                    name = row['Name'].replace("'","''")
                    Id = row['ID']
                    pos = row['Pos']
                    Type = row['Type']

                    find_key_sql = "SELECT key_person FROM chadwickbureau WHERE key_bbref_minors = '{0}'".format(Id)
                    out_f.write(find_key_sql + "\n")

                    cur.execute(find_key_sql)
                    key_person = ''
                    for result in cur:
                        key_person = result[0]

                    insert_sql = "INSERT INTO draft (year, rnd, ovpick, team, name, key_person, pos, signed, type) "
                    insert_sql += "VALUES ({0}, {1}, {2}, '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')".format(year, rnd, ovpck, tm, name, key_person, pos, signed, Type)
                    out_f.write(insert_sql + "\n")

                    cur.execute(insert_sql)
                    cur.connection.commit()
                #with f.open() as round_f:

                out_f.write("done")

        #conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)
        #cur = conn.cursor()
        #cur.execute(sql)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/{0}/round_{1}.sql'.format(self.year, self.rnd))


class ScrapeYearRound(luigi.Task):
    year = luigi.Parameter()
    rnd = luigi.Parameter()

    def run(self):
        url = url_template.format(self.year, self.rnd)

        page = urllib2.urlopen(url).read()

        soup = BeautifulSoup(page, 'lxml')

        minors_re = re.compile('minors')
        id_re = re.compile('id=(.+)$')

        table = soup.find(lambda tag: tag.name=='table' and tag.has_key('id') and tag['id']=='draft_stats')

        thead = table.find('thead')
        tbody = table.find('tbody')

        with self.output().open('wb') as out_file:
            csv_writer = csv.writer(out_file)
            header = []
            tr = thead.find_all('tr')[0]
            for th in tr.find_all('th'):
                header.append(th.text)
                if th.text == 'Name':
                    header.append('ID')
            csv_writer.writerow(header)

            for tr in tbody.find_all('tr'):
                pick = []
                for td in tr.find_all('td'):
                    pick.append(td.text.encode('utf-8'))
                    if minors_re.search(td.text):
                        id_found = False
                        for a in td.find_all('a'):
                            m = id_re.search(a['href'])
                            if m:
                                pick.append(m.group(1))
                                id_found = True

                        if not id_found:
                            pick.append('')
                                

                csv_writer.writerow(pick)

        time.sleep(5)


    def output(self):
        return luigi.LocalTarget('/home/dan/data/draft/{0}/round_{1}.csv'.format(self.year, self.rnd))
