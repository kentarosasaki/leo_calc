#!/usr/bin/env python
# coding=utf-8
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
bucket_calc.py: LeoFS Bucket Size Calculation Tools.
"""


__author__ = "Kentaro Sasaki"
__copyright__ = "Copyright 2015 Kentaro Sasaki"


import MySQLdb
import argparse
import fileinput
import sys


class DB(object):
  def __init__(self, host, user, password, db):
    self.host = host
    self.user = user
    self.password = password
    self.db = db
    self.connect()

  def connect(self):
    try:
      self.conn = MySQLdb.connect(self.host, self.user, self.password, self.db)
    except (AttributeError, MySQLdb.OperationalError), e:
      raise e

  def query(self, sql, params = ()):
    try:
      cursor = self.conn.cursor()
      cursor.execute(sql, params)
    except (AttributeError, MySQLdb.OperationalError) as e:
      print("exception generated during sql connection: %s", str(e))
      self.connect()
      cursor = self.conn.cursor()
      cursor.execute(sql, params)
    return cursor

  def close(self):
    try:
      if self.conn:
        self.conn.commit()
        self.conn.close()
      else:
        print("...No Database Connection to Close.")
    except (AttributeError, MySQLdb.OperationalError) as e:
      raise e


def create_talbe_if_not_exists():
  sql = ("CREATE TABLE IF NOT EXISTS `leofs_keys` \
         (`bucket` varchar(255) NOT NULL, `path` varchar(255) NOT NULL, \
         `size` bigint(20) NOT NULL, `unix_t` bigint(20) NOT NULL, \
         PRIMARY KEY (`path`)) ENGINE=InnoDB DEFAULT CHARSET=utf8")
  return sql


def insert_log():
  sql = ("INSERT INTO leofs_keys (path, bucket, size, unix_t) \
         VALUES (%s, %s, %s, %s)")
  return sql


def update_log():
  sql = ("UPDATE leofs_keys SET size = %s, unix_t = %s WHERE path = %s")
  return sql


def update_add_log():
  sql = ("UPDATE leofs_keys SET size = size + %s, unix_t = %s WHERE path = %s")
  return sql


def delete_log():
  sql = ("DELETE FROM leofs_keys WHERE path = %s")
  return sql


def select_log():
  sql = ("SELECT unix_t FROM leofs_keys WHERE path = %s")
  return sql


def bucket_list():
  sql = ("SELECT DISTINCT bucket FROM leofs_keys")
  return sql


def bucket_size():
  sql = ("SELECT sum(size) FROM leofs_keys WHERE bucket = %s")
  return sql


def bucket_object():
  sql = ("SELECT count(*) FROM leofs_keys WHERE bucket = %s")
  return sql


def regist_log(args):
  buff = [i.split('\t') for i in fileinput.input(args.file)]
  db = DB(args.host, args.user, args.password, args.db)
  db.query(create_talbe_if_not_exists())
  for b in buff:
    result = db.query(select_log(), b[2])
    if result.fetchall():
      for r in result:
        if "PUT" in b[0]:
          if b[6] > str(r[0]):
            if b[3] <= "1":
              db.query(update_log(), (b[4], b[6], b[2]))
            else:
              db.query(update_add_log(), (b[4], b[6], b[2]))
        elif "DELETE" in b[0]:
          if b[6] >= str(r[0]):
            db.query(delete_log(), b[2])
    else:
      db.query(insert_log(), (b[2], b[1], b[4], b[6]))
  db.close()


def analyze_log(args):
  db = DB(args.host, args.user, args.password, args.db)
  fp = args.output
  fp.write("+------------------------------------------------+--------------------+--------------------+\n")
  fp.write("|                     Bucket                     |       Object       |       Size(GB)     |\n")
  fp.write("+------------------------------------------------+--------------------+--------------------+\n")
  b_list = db.query(bucket_list())
  for b in b_list:
    b_object = [o for o in db.query(bucket_object(), b[0])]
    b_size = [s for s in db.query(bucket_size(), b[0])]
    fp.write("|%-48s|%20d|%20d|\n" %
             (b[0], b_object[0][0], b_size[0][0] / 1024 / 1024 / 1024))
  fp.write("+------------------------------------------------+--------------------+--------------------+\n")
  db.close()


def main():
  parser = argparse.ArgumentParser(
      description='LeoFS Bucket Calculation Tool.',
      formatter_class=argparse.RawDescriptionHelpFormatter)

  parser.add_argument('-H', '--host', metavar='host', type=str,
                      default="localhost", help='Host name, IP Address')
  parser.add_argument('-u', '--user', metavar='user', type=str,
                      required=True, help='connect using the indicated username')
  parser.add_argument('-p', '--password', metavar='password',
      type=str, required=True,
      help='use the indicated password to authenticate the connection')
  parser.add_argument('-d', '--db', metavar='db', type=str,
                      required=True, help='Check database with indicated name')

  subparsers = parser.add_subparsers(help='sub-command help')

  parser_regist = subparsers.add_parser('regist', help="Regist object logs")
  parser_regist.add_argument('-f', '--file', metavar='file', nargs='*',
                             required=True, help='Log files to insert or update')
  parser_regist.set_defaults(func=regist_log)

  parser_size = subparsers.add_parser('size', help="Output stored object size")
  parser_size.add_argument('-o', '--output', metavar='output',
                           type=argparse.FileType('w'), default=sys.stdout)
  parser_size.set_defaults(func=analyze_log)

  args = parser.parse_args()

  args.func(args)


if __name__ == "__main__":
  main()
