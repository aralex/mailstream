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

parser.add_argument('-c', dest = 'cfg_file', nargs = 1, help = 'configuration file name')
parser.add_argument('-t', dest = 'tpl_file', nargs = '?', type = argparse.FileType(), help = 'message template file')
parser.add_argument('-d', dest = 'data_file', nargs = '?', type = argparse.FileType(), help = 'data file')
parser.add_argument('-s', dest = 'skip_lines', nargs = '?', type = int, required = False, default = -1, help = 'skip first N lines')
parser.add_argument('-q', dest = 'quiet_mode', required = False, action='store_true', help = 'quiet mode')
parser.add_argument('-p', dest = 'print_mode', required = False, action='store_true', help = 'printing mode')
parser.add_argument('-D', dest = 'dry_run_mode', required = False, action='store_true', help = 'dry run mode')

options = parser.parse_args()

cfg_file = options.cfg_file[0]
tpl_file = options.tpl_file
data_file = options.data_file or sys.stdin
verbose_mode = not options.quiet_mode or False
print_mode = options.print_mode or False
dry_run = options.dry_run_mode or False

if verbose_mode:
  print 'Configuration file: ' + cfg_file
  

config = ConfigParser.ConfigParser()
config.read(cfg_file)

smtp_server = config.get('server', 'hostname')
smtp_user = config.get('server', 'user')
smtp_password = config.get('server', 'password')
msg_sender = config.get('server', 'sender')
msg_delay = config.getint('server', 'delay')
msgs_processed = config.getint('server', 'processed')
reconnect_after = config.getint('server', 'reconnect_after')
reconnect_delay = config.getint('server', 'reconnect_delay')


if options.skip_lines is None:
  lines_to_skip = msgs_processed
elif options.skip_lines == -1:  
  lines_to_skip = 0
else:
  lines_to_skip = options.skip_lines


if verbose_mode:
  print 'Lines to skip: %d' % (lines_to_skip)


if lines_to_skip == -1:
  lines_to_skip = msgs_processed

LTS = lines_to_skip

n = 0

with tpl_file:
  tpl = tpl_file.read()


reader = csv.DictReader(data_file, delimiter=';')


# Соединяемся с smtp-сервером.
server = smtplib.SMTP_SSL(smtp_server)
#server.set_debuglevel(1)
server.login(smtp_user, smtp_password)


# Цикл по наборам данных
for row in reader:
  vTo = row['email']
  
  if lines_to_skip > 0:
    lines_to_skip = lines_to_skip - 1
    n = n + 1
    print 'Skipped %d of %d: %s' % (n, LTS, vTo)
    continue
  
  date = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
  
  message = tpl

  message = re.sub('{date}', date, message)
  message = re.sub('{from}', msg_sender, message)

  fields = re.findall(r"\{(\w+)\}", message)
  
  names = row['name'].split(' ')
  row['name1'] = names[1] if (len(names) > 1) else row['name']

  if len(fields) > 0:
    for f in fields:
      value = '{' + f + '}';
      message = re.sub(value, row[f], message)

  vFrom = msg_sender
  vBody = message

  if not dry_run:
    server.sendmail(vFrom, vTo, vBody)
    
  n = n + 1
  
  if print_mode:
    print '%s\n---------------------------------------------\n' % (vBody)

    
  config.set('server', 'processed', n)
  with open(cfg_file, 'wb') as configfile:
    config.write(configfile)
  
  if not dry_run:
    if (n % reconnect_after) == 0:
      server.quit()
      time.sleep(4)
      server = smtplib.SMTP_SSL(smtp_server)
      server.login(smtp_user, smtp_password)
    else:
      time.sleep(msg_delay)
    
  if verbose_mode:
    print '%d: mail sent to %s, %s %s' % (n, vTo, date, '- simulated' if dry_run else '')
    
if not dry_run:
  server.quit()

config.set('server', 'processed', -1)
with open(cfg_file, 'wb') as configfile:
  config.write(configfile)
