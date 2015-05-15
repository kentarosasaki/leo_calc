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
log_merger.py: Merge logs of all LeoFS Gateway
"""


__author__ = "Kentaro Sasaki"
__copyright__ = "Copyright 2015 Kentaro Sasaki"


import argparse
import fileinput
import sys


def extract_timestamp(line):
  """Extract timestamp and convert to a form that gives the
  expected result in a comparison
  """
  # return unixtime value
  return line.split('\t')[6]


EXAMPLES = """Examples:

  When merging logs:

    # Content of access.1.log
    [PUT]    bucket001 bucket001/file_768K 0 786432 2015-03-31 23:58:03.825296 +0900 1427813883825366 200
    [DELETE] bucket001 bucket001/file_32K  0 32768  2015-03-31 23:59:04.87913  +0900 1427813944087985 200

    # Content of access.2.log
    [PUT]    bucket001 bucket001/file_16K  0 16384  2015-03-31 23:59:04.131844 +0900 1427813944131908 200
    [GET]    bucket001 bucket001/file_192K 0 196608 2015-03-31 23:59:04.167192 +0900 1427813944167258 200

    # Merge the two log files
    ./log_merger.py access.1.log access.2.log

    # Results
    [PUT]    bucket001 bucket001/file_768K 0 786432 2015-03-31 23:58:03.825296 +0900 1427813883825366 200
    [DELETE] bucket001 bucket001/file_32K  0 32768  2015-03-31 23:59:04.87913  +0900 1427813944087985 200
    [PUT]    bucket001 bucket001/file_16K  0 16384  2015-03-31 23:59:04.131844 +0900 1427813944131908 200

"""


def main():
  parser = argparse.ArgumentParser(
      description='Merge LeoFS Gateway access log by unix timestamps.',
      epilog=EXAMPLES,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('file', metavar='file', nargs='*', help='Log files to merge')
  parser.add_argument('-o', '--output', metavar='output_log',
                      type=argparse.FileType('w'), default=sys.stdout)
  args = parser.parse_args()

  tupledlist = [(extract_timestamp(l), l) for l in fileinput.input(args.file)
                if "[PUT]" in l or "[DELETE]" in l]
  # Sort by unixtime
  tupledlist.sort()
  for sortedlist in tupledlist:
    args.output.write(sortedlist[1])


if __name__ == '__main__':
  main()
