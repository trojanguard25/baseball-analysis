import luigi
import pymysql
import csv
import re
import smtplib

import sys
import math
import time

sys.path.append('/home/dan/github/player-finder/src/')
from config import Config

class GetHitterDailyEvents(luigi.Task):
    key_mlbam = luigi.Parameter()
    date = luigi.DateParameter()

    def run(self):
        with self.output().open('w') as out_f:
            conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

            name_sql = 'select name_first, name_last from chadwickbureau where key_mlbam = %s' % (self.key_mlbam)
            cur = conn.cursor()
            cur.execute(name_sql)

            for result in cur:
                name = "{0} {1}".format(result[0], result[1])
            
            out_f.write(name + "\n")

            find_sql = 'select a.gameName, a.des, a.`event` from gameday.atbats a'
            find_sql += ' join gameday.gameDetail g on a.gameName = g.gameName'
            find_sql += ' where a.batter = {0} and STR_TO_DATE(g.original_date, \'%Y/%m/%d\') = \'{1}\''.format(str(self.key_mlbam), str(self.date))
            cur = conn.cursor()
            cur.execute(find_sql)

            for result in cur:
                line = result[1]
                line += '\t{0}\n'.format(result[2])
                out_f.write(line)

    def output(self):
        return luigi.LocalTarget('/home/dan/data/daily_events/{0}/{1}.txt'.format(self.date, self.key_mlbam))

class GetDailyEvents(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        conn = pymysql.connect(host=Config.dbHost, port=Config.dbPort, user=Config.dbUser, passwd=Config.dbPass, db=Config.dbName)

        hitter_sql = 'select key_mlbam from player_tasks where hitter = \'Y\' and daily_events > 0'
        pitcher_sql = 'select key_mlbam from player_tasks where pitcher = \'Y\' and daily_events > 0'

        cur = conn.cursor()
        cur.execute(hitter_sql)

        requirements = {'hitters' : [GetHitterDailyEvents(result[0], self.date) for result in cur]}

        cur.execute(pitcher_sql)
        #requirements = {'pitchers' : [GetPitcherDailyEvents(result[0], self.date) for result in cur]}
        return requirements

    def run(self):
        with self.output().open('w') as out_f:
            for t in self.input()['hitters']:
                with t.open('r') as in_f:
                    for line in in_f:
                        out_f.write(line)
                    out_f.write('\n')

    def output(self):
        return luigi.LocalTarget('/home/dan/data/daily_events/{0}/summary.txt'.format(self.date))

class EmailDailyEvents(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return GetDailyEvents(self.date)

    def run(self):
        body = ''
        with self.input().open('r') as in_f:
            for line in in_f:
                body += line

        user = 'prospectnavigator@gmail.com'
        password = '0ldn0teb00k'
        subject = 'Daily Update - %s' % (self.date)
        sent_from = 'prospectnavigator@gmail.com'
        to = 'dan.rausch@gmail.com'
        email_text = """\
From: %s
To: %s
Subject: %s

Daily Update for %s.
%s
""" % (sent_from, to, subject, self.date, body)

        #try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(user, password)
        server.sendmail(sent_from, to, email_text)
        server.close()

        print 'Email sent!'

        #except:
        #    print 'Something went wrong...'

