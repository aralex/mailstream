#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import csv
import smtplib
import datetime
import time
import argparse
import ConfigParser


parser = argparse.ArgumentParser(prog = 'mailstream', description = ('Mail stream.'))

parser.add_argument('-c', dest = 'cfg_file', nargs = 1, #type=argparse.FileType(),
                    help = 'configuration file name')

parser.add_argument('-t', dest = 'tpl_file', nargs = 1, type = argparse.FileType(), help = 'message template file')

parser.add_argument('-s', dest = 'skip_lines', nargs = '?', type = int, required = False, default = -1, help = 'skip first N lines')

options = parser.parse_args()

cfg_file = options.cfg_file[0]
tpl_file = options.tpl_file[0]
lines_to_skip = options.skip_lines or 0

print 'cfg_file: ' + cfg_file
print 'lines_to_skip: %d' % (lines_to_skip)

exit

#cfg_file = 'smtp_tarantella.cfg'

config = ConfigParser.ConfigParser()
config.read(cfg_file)

cSmtpServer = config.get('server', 'hostname')
cUser = config.get('server', 'user')
cPassword = config.get('server', 'password')
cFrom = config.get('server', 'sender')
cDelay = config.getint('server', 'delay')
cProcessed = config.getint('server', 'processed')

if lines_to_skip == -1:
  lines_to_skip = cProcessed

LTS = lines_to_skip
dry_run = False

n = 1

with tpl_file:
  tpl = tpl_file.read()


reader = csv.DictReader(sys.stdin, delimiter=';')


# Соединяемся с smtp-сервером.
server = smtplib.SMTP_SSL(cSmtpServer)
#server.set_debuglevel(1)
server.login(cUser, cPassword)


# Цикл по наборам данных
for row in reader:
  vTo = row['email']
  
  if lines_to_skip > 0:
    lines_to_skip = lines_to_skip - 1
    print 'Skipped %d of %d: %s' % (n, LTS, vTo)
    n = n + 1
    continue
  
  date = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
  
  message = tpl

  message = re.sub('{date}', date, message)
  message = re.sub('{from}', cFrom, message)

  fields = re.findall(r"\{(\w+)\}", message)
  
  names = row['name'].split(' ')
  row['name1'] = names[1] if (len(names) > 1) else row['name']

  if len(fields) > 0:
    for f in fields:
      value = '{' + f + '}';
      message = re.sub(value, row[f], message)

  vFrom = cFrom
  vBody = message
  
  if dry_run:
    print '[%d: %s, %s]' % (n, vTo, date)
    n = n + 1
    continue
    

  server.sendmail(vFrom, vTo, vBody)
  print '%d: mail sent to %s, %s' % (n, vTo, date)
  n = n + 1
  config.set('server', 'processed', n)
  with open(cfg_file, 'wb') as configfile:
    config.write(configfile)
  
  if (n % 44) == 0:
    server.quit()
    time.sleep(4)
    server = smtplib.SMTP_SSL(cSmtpServer)
    server.login(cUser, cPassword)
  else:
    time.sleep(cDelay)
    
server.quit()
