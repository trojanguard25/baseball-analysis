import sys
import luigi
import pymysql
import csv
import Levenshtein
import re
import time

import urllib2
from bs4 import BeautifulSoup

sys.path.append('/home/dan/github/player-finder/src/')
from baseballReferenceSource import BRSource
from config import Config

class GetYearPlayers(luigi.Task):
    year = luigi.Parameter()

    def run(self):
        with self.output().open('wb') as out_f:
            out_writer = csv.writer(out_f)
            header = ['key_person', 'name_last', 'name_first']
            out_writer.writerow(header)

            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)
            cur = conn.cursor()
            find_sql = 'select c.key_person, c.name_last, c.name_first from chadwickbureau c where c.pro_played_first <= {0} and c.pro_played_last >= {0}'.format(str(self.year))
            cur.execute(find_sql)
            for result in cur:
                line = []
                line.append(result[0])
                line.append(result[1])
                line.append(result[2])
                out_writer.writerow(line)
    
    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/player_lists/{0}.csv'.format(str(self.year)))

class GetPlayerKey(luigi.Task):
    name = luigi.Parameter()
    rank = luigi.Parameter()
    year = luigi.Parameter()

    def requires(self):
        return CalcPlayerKey(self.name, self.rank, self.year)

    def run(self):
        with self.output().open('wb') as out_f:
            out_writer = csv.writer(out_f)
            header = ['key_person']
            out_writer.writerow(header)

            with self.input().open('r') as in_f:
                in_csv = csv.DictReader(in_f)
                first = True
                first_uuid = ''
                for row in in_csv:
                    uuid = row['key_person']
                    score = row['score']
                    if first and int(score) != 0:
                        raise ValueError("score not equal 0")
                    elif not first and int(score) == 0:
                        raise ValueError("more than one score equal 0")

                    if first:
                        first_uuid = uuid
                        first = False
            out_writer.writerow([first_uuid])
    
    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/{0}/{1}.csv'.format(self.year, self.rank))


class CalcPlayerKey(luigi.Task):
    name = luigi.Parameter()
    rank = luigi.Parameter()
    year = luigi.Parameter()

    def requires(self):
        return GetYearPlayers(self.year)

    def run(self):
        best = {'score' : 99, 'name' : 'dummy', 'key_person': 'dummy'}
        good = []
        with self.output().open('wb') as out_f:
            out_writer = csv.writer(out_f)
            header = ['key_person', 'input_name', 'best_name', 'score']
            out_writer.writerow(header)

            with self.input().open('r') as in_f:
                in_csv = csv.DictReader(in_f)
                for row in in_csv:
                    uuid = row['key_person']
                    name_first = row['name_first']
                    name_last = row['name_last']

                    name = name_first + ' ' + name_last
                    
                    score = Levenshtein.distance(name.lower(), self.name.lower())

                    if score < best['score']:
                        if best['score'] - score > 2:
                            good = []
                        else:
                            good.append([best['key_person'], self.name, best['name'], best['score']])
                        best['score'] = score
                        best['name'] = name
                        best['key_person'] = uuid
                    elif score - best['score'] < 3:
                        good.append([uuid, self.name, name, score])


            output = [best['key_person'], self.name, best['name'], best['score']]
            out_writer.writerow(output)
            for row in good:
                if row[3] - best['score'] < 3:
                    out_writer.writerow(row)
    
    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/guesses/{0}/{1}.csv'.format(self.year, self.rank))

                    
class FindPlayerKeys(luigi.Task):
    def requires(self):
        keys = []
        with open('/home/dan/BATop100Lists.csv','r') as in_f:
            mycsv = csv.DictReader(in_f)
            for row in mycsv:
                name = row['name']
                rank = int(row['rank'])
                year = int(row['year'])
                keys.append(CalcPlayerKey(name, rank, year))
        return keys

class CalcPlayerSurplusValuesTPOP(luigi.Task):
    def requires(self):
        keys = {}
        with open('/home/dan/BATop100Lists.csv','r') as in_f:
            mycsv = csv.DictReader(in_f)
            for row in mycsv:
                name = row['name']
                rank = int(row['rank'])
                year = int(row['year'])
                if year > 2007 or year < 1994:
                    continue

                if rank not in keys:
                    keys[rank] = {}
                    keys[rank] = {year : GetPlayerKey(name, rank, year)}
                else:
                    keys[rank][year] = GetPlayerKey(name, rank, year)
        return keys

    def run(self):
        br = BRSource()
        discount = 0.92
        min_salary = 0.55
        per_war = 8.0
        minor_league = 0
        with self.output().open('wb') as out_f:
            header = ['rank','year','key_person','total_war','discounted_war','surplus_value']
            csv_writer = csv.writer(out_f)
            csv_writer.writerow(header)

            for rank, t in self.input().iteritems():
                for year, inp in t.iteritems():
                    with inp.open() as in_f:
                        with inp.open('r') as in_f:
                            mycsv = csv.DictReader(in_f)
                            key_person = ''
                            for uuid in mycsv:
                                key_person = uuid['key_person']
                                break
                           
                            war = br.getCombinedWar(key_person)
                            if not war.years:
                                minor_league += 1
                                csv_writer.writerow([str(rank), str(year), key_person, '0', '0', '0'])
                                continue

                            zero_year = year
                            if year - 1 in war._year_wars:
                                zero_year = year - 1
                            else:
                                while zero_year not in war._year_wars and zero_year < 2011:
                                    zero_year += 1

                            curr_year = year
                            per = 1.0
                            discounted_total_war = 0.0
                            total_war = 0.0
                            salary = 0.0

                            '''
                            while curr_year <= zero_year + 6:
                                if curr_year in war._year_wars:
                                    war_year = war._year_wars[curr_year].bat_war + war._year_wars[curr_year].pitch_war
                                    discounted_war = per * war_year

                                    total_war += war_year
                                    discounted_total_war += discounted_war

                                    if curr_year - zero_year <= 3 and curr_year - zero_year > 0:
                                        salary += min_salary
                                    elif curr_year - zero_year == 4:
                                        salary += max(discounted_war * per_war * 0.25, min_salary)
                                    elif curr_year - zero_year == 5:
                                        salary += max(discounted_war * per_war * 0.40, min_salary)
                                    elif curr_year - zero_year == 6:
                                        salary += max(discounted_war * per_war * 0.60, min_salary)

                                curr_year += 1
                                per *= discount
                
                            surplus_value = discounted_total_war * per_war - salary
                            '''

                            while curr_year <= zero_year + 6:
                                if curr_year in war._year_wars:
                                    war_year = war._year_wars[curr_year].bat_war + war._year_wars[curr_year].pitch_war
                                    discounted_war = per * war_year

                                    total_war += war_year

                                    if curr_year - zero_year <= 3 and curr_year - zero_year > 0:
                                        salary += min_salary
                                    elif curr_year - zero_year == 4:
                                        salary += max(discounted_war * per_war * 0.25, min_salary)
                                    elif curr_year - zero_year == 5:
                                        salary += max(discounted_war * per_war * 0.40, min_salary)
                                    elif curr_year - zero_year == 6:
                                        salary += max(discounted_war * per_war * 0.60, min_salary)

                                curr_year += 1
                                per *= discount
                            discounted_total_war = 0.82 * total_war
                            salary = 0.42 * 0.6 * discounted_total_war * per_war + 3 * min_salary
                            surplus_value = discounted_total_war * per_war - salary

                            csv_writer.writerow([str(rank), str(year), key_person, str(total_war), str(discounted_total_war), str(surplus_value)])
        
    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/surplus_values_tpop.csv')


class CalcPlayerSurplusValues(luigi.Task):
    def requires(self):
        keys = {}
        with open('/home/dan/BATop100Lists.csv','r') as in_f:
            mycsv = csv.DictReader(in_f)
            for row in mycsv:
                name = row['name']
                rank = int(row['rank'])
                year = int(row['year'])
                if year > 2007 or year < 1994:
                    continue

                if rank not in keys:
                    keys[rank] = {}
                    keys[rank] = {year : GetPlayerKey(name, rank, year)}
                else:
                    keys[rank][year] = GetPlayerKey(name, rank, year)
        return keys

    def run(self):
        br = BRSource()
        discount = 0.92
        min_salary = 0.55
        per_war = 8.0
        minor_league = 0
        with self.output().open('wb') as out_f:
            header = ['rank','year','key_person','total_war','discounted_war','surplus_value']
            csv_writer = csv.writer(out_f)
            csv_writer.writerow(header)

            for rank, t in self.input().iteritems():
                for year, inp in t.iteritems():
                    with inp.open() as in_f:
                        with inp.open('r') as in_f:
                            mycsv = csv.DictReader(in_f)
                            key_person = ''
                            for uuid in mycsv:
                                key_person = uuid['key_person']
                                break
                           
                            war = br.getCombinedWar(key_person)
                            if not war.years:
                                minor_league += 1
                                csv_writer.writerow([str(rank), str(year), key_person, '0', '0', '0'])
                                continue

                            zero_year = year
                            if year - 1 in war._year_wars:
                                zero_year = year - 1
                            else:
                                while zero_year not in war._year_wars and zero_year < 2011:
                                    zero_year += 1

                            curr_year = year
                            per = 1.0
                            discounted_total_war = 0.0
                            total_war = 0.0
                            salary = 0.0

                            while curr_year <= zero_year + 6:
                                if curr_year in war._year_wars:
                                    war_year = war._year_wars[curr_year].bat_war + war._year_wars[curr_year].pitch_war
                                    discounted_war = per * war_year

                                    total_war += war_year
                                    discounted_total_war += discounted_war

                                    if curr_year - zero_year <= 3 and curr_year - zero_year > 0:
                                        salary += min_salary
                                    elif curr_year - zero_year == 4:
                                        salary += max(discounted_war * per_war * 0.25, min_salary)
                                    elif curr_year - zero_year == 5:
                                        salary += max(discounted_war * per_war * 0.40, min_salary)
                                    elif curr_year - zero_year == 6:
                                        salary += max(discounted_war * per_war * 0.60, min_salary)

                                curr_year += 1
                                per *= discount
                
                            surplus_value = discounted_total_war * per_war - salary
                            csv_writer.writerow([str(rank), str(year), key_person, str(total_war), str(discounted_total_war), str(surplus_value)])
        
    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/surplus_values.csv')

class BinSurplusValues(luigi.Task):
    def requires(self):
        return CalcPlayerSurplusValues()
        #return CalcPlayerSurplusValuesTPOP()

    def run(self):
        br = BRSource()
        problems = 0

        with self.output().open('wb') as out_f:
            header = ['range','count','average_total_war','surplus_value']
            csv_writer = csv.writer(out_f)
            csv_writer.writerow(header)

            average_war = {
                    'H 1-10' : { 'count' : 0, 'war' : 0 , 'sv' : 0},
                    'H 11-25' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'H 26-50' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'H 51-75' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'H 76-100' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'P 1-10' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'P 11-25' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'P 26-50' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'P 51-75' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'P 76-100' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'Q 1-10' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'Q 11-25' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'Q 26-50' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'Q 51-75' : { 'count' : 0, 'war' : 0, 'sv' : 0 },
                    'Q 76-100' : { 'count' : 0, 'war' : 0, 'sv' : 0 }
                    }
            '''
            average_sv = {
                    'H 1-10' : { 'count' : 0, 'total' : 0 },
                    'H 11-25' : { 'count' : 0, 'total' : 0 },
                    'H 26-50' : { 'count' : 0, 'total' : 0 },
                    'H 51-75' : { 'count' : 0, 'total' : 0 },
                    'H 76-100' : { 'count' : 0, 'total' : 0 },
                    'P 1-10' : { 'count' : 0, 'total' : 0 },
                    'P 11-25' : { 'count' : 0, 'total' : 0 },
                    'P 26-50' : { 'count' : 0, 'total' : 0 },
                    'P 51-75' : { 'count' : 0, 'total' : 0 },
                    'P 76-100' : { 'count' : 0, 'total' : 0 }
                    }
                    '''
            last_years = {}
            with self.input().open('r') as in_f:
                mycsv = csv.DictReader(in_f)
                for row in mycsv:
                    uuid = row['key_person']
                    if uuid not in last_years:
                        last_years[uuid] = int(row['year'])
                    else:
                        if int(row['year']) > last_years[uuid]:
                            last_years[uuid] = int(row['year'])

            with self.input().open('r') as in_f:
                mycsv = csv.DictReader(in_f)
                for row in mycsv:
                    uuid = row['key_person']
                    pos = 'H' if br.isHitter(uuid) else 'P'

                    if not br.isHitter(uuid) and not br.isPitcher(uuid):
                        pos = 'Q'
                        problems += 1

                    ran = '1-10'
                    if int(row['rank']) >= 11 and int(row['rank']) <= 25:
                        ran = '11-25'
                    elif int(row['rank']) >= 26 and int(row['rank']) <= 50:
                        ran = '26-50'
                    elif int(row['rank']) >= 51 and int(row['rank']) <= 75:
                        ran = '51-75'
                    elif int(row['rank']) >= 76 and int(row['rank']) <= 100:
                        ran = '76-100'

                    #if last_years[uuid] == int(row['year']):
                    key = '{0} {1}'.format(pos, ran)
                    average_war[key]['count'] += 1
                    average_war[key]['war'] += float(row['total_war'])
                    average_war[key]['sv'] += float(row['surplus_value'])

                for k,v in average_war.iteritems():
                    if v['count'] > 0:
                        result = [k,str(v['count']), str(v['war']/v['count']), str(v['sv']/v['count'])]
                        csv_writer.writerow(result)
                    
        print problems

    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/results.csv')


                     
class CheckPlayerKeys(luigi.Task):
    def requires(self):
        keys = []
        with open('/home/dan/BATop100Lists.csv','r') as in_f:
            mycsv = csv.DictReader(in_f)
            for row in mycsv:
                name = row['name']
                rank = int(row['rank'])
                year = int(row['year'])
                keys.append(GetPlayerKey(name, rank, year))
        return keys

    def run(self):
        br = BRSource()
        missing_position = {}
        with self.output().open('wb') as out_f:
            out_writer = csv.writer(out_f)
            header = ['key_person']
            out_writer.writerow(header)
            for t in self.input():
                with t.open('r') as in_f:
                    mycsv = csv.DictReader(in_f)
                    for row in mycsv:
                        uuid = row['key_person']
                        if not br.isHitter(uuid) and not br.isPitcher(uuid):
                            if uuid not in missing_position:
                                out_writer.writerow([uuid])
                                missing_position[uuid] = ''


    def output(self):
        return luigi.LocalTarget('/home/dan/data/surplus_value/no_position.csv')
           
class FindPositions(luigi.Task):
    url_template = 'http://www.baseball-reference.com/register/player.fcgi?id={0}'

    def requires(self):
        return CheckPlayerKeys()

    def run(self):
        br = BRSource()
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)
        with self.input().open('r') as in_f:
            mycsv = csv.DictReader(in_f)
            for row in mycsv:
                uuid = row['key_person']
                if not br.isHitter(uuid) and not br.isPitcher(uuid):
                    find_sql = 'SELECT key_bbref_minors FROM chadwickbureau where key_person=\'{0}\''.format(uuid)
                    cur = conn.cursor()
                    cur.execute(find_sql)

                    for result in cur:
                        bbref_key = result[0]

                    url = self.url_template.format(bbref_key)
                    page = urllib2.urlopen(url).read()
                    soup = BeautifulSoup(page, 'lxml')
                    position_re = re.compile('Position')
                    pitcher_re = re.compile('Pitcher')

                    div = soup.find(lambda tag: tag.name=='div' and tag.has_key('id') and tag['id']=='meta')

                    insert_sql = "INSERT INTO is_pitcher (key_person, pitcher, hitter) values ('{0}','{1}','{2}')"
                    pitcher = False
                    for p in div.find_all('p'):
                        if position_re.search(p.text):
                            if pitcher_re.search(p.text):
                                pitcher = True
                                break

                    if pitcher:
                        print "pitcher!"
                        cur.execute(insert_sql.format(uuid, 'Y', 'N'))
                    else:
                        print "batter!"
                        cur.execute(insert_sql.format(uuid, 'N', 'Y'))
                    cur.connection.commit()
                    
                    time.sleep(5)
