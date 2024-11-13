#!/usr/bin/env impala-python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script is used to restore table stats in planner test file.
# Restoring table stats makes code review somewhat easier because
# they are never / rarely verified by any tests.

from __future__ import absolute_import, division, print_function
import glob
import os
import re

WORKLOAD_DIR = os.environ["IMPALA_WORKLOAD_DIR"]

PATH_TO_REPLACE = {
  "functional-planner/queries/PlannerTest/tpcds": None,
  "functional-planner/queries/PlannerTest": [
    "agg-node-high-mem-estimate.test",
    "agg-node-low-mem-estimate.test",
    "agg-node-max-mem-estimate.test",
    "processing-cost-plan-admission-slots.test",
    "tpcds-processing-cost.test",
  ]
}

FIXED_STATS = {
  "tpcds_parquet": [
    ("call_center", "6", "10.28KB"),
    ("catalog_page", "11.72K", "739.17KB"),
    ("catalog_returns", "144.07K", "10.62MB"),
    ("catalog_sales", "1.44M", "96.62MB"),
    ("customer", "100.00K", "5.49MB"),
    ("customer_address", "50.00K", "1.16MB"),
    ("customer_demographics", "1.92M", "7.49MB"),
    ("date_dim", "73.05K", "2.15MB"),
    ("household_demographics", "7.20K", "41.69KB"),
    ("income_band", "20", "1.22KB"),
    ("inventory", "11.74M", "34.09MB"),
    ("item", "18.00K", "1.73MB"),
    ("promotion", "300", "23.30KB"),
    ("reason", "35", "1.92KB"),
    ("ship_mode", "20", "2.68KB"),
    ("store", "12", "9.93KB"),
    ("store_returns", "287.51K", "15.43MB"),
    ("store_sales", "2.88M", "200.96MB"),
    ("time_dim", "86.40K", "1.31MB"),
    ("warehouse", "5", "4.38KB"),
    ("web_page", "60", "5.56KB"),
    ("web_returns", "71.76K", "5.66MB"),
    ("web_sales", "719.38K", "45.09MB"),
    ("web_site", "30", "11.91KB")
  ]
}
FIXED_STATS["tpcds_partitioned_parquet_snap"] = FIXED_STATS["tpcds_parquet"]

TABLE_STATS = dict()
for db, tables in FIXED_STATS.items():
  for (table, rows, bytes) in tables:
    TABLE_STATS["{}.{}".format(db, table)] = (rows, bytes)

RE_SCAN = re.compile(r".*:SCAN.*\[([_a-z]+).([_a-z]+)(.*)\]")
RE_PARTITION = re.compile(r"(.*) partitions=(\d+)/(\d+) files=(\d+) size=(\d.*)")
RE_HMS_STATS = re.compile(r"(.*) table: rows=(.*) size=(\d.*)")

test_files = list()
for path_to_check, files_to_check in PATH_TO_REPLACE.items():
  if not files_to_check:
    dir_path = os.path.join(WORKLOAD_DIR, path_to_check)
    test_files.extend(glob.glob(dir_path + "/*.test"))
  else:
    for f in files_to_check:
      test_files.append(os.path.join(WORKLOAD_DIR, path_to_check, f))

for f in test_files:
  lines = list()
  with open(f) as fd:
    lines = fd.readlines()

  print("Restoring {} ...".format(f))
  with open(f, 'w') as fd:
    stats = None
    for line in lines:
      m = RE_SCAN.match(line)
      if m:
        full_table = m.group(1) + "." + m.group(2)
        if full_table in TABLE_STATS:
          stats = TABLE_STATS[full_table]
          fd.write(line)
          continue
        else:
          stats = None

      new_line = line
      m = RE_PARTITION.match(line)
      if m and stats:
        new_line = "{} partitions={}/{} files={} size={}\n".format(
          m.group(1), m.group(2), m.group(3), m.group(4), stats[1]
        )

      m = RE_HMS_STATS.match(line)
      if m and stats:
        new_line = "{} table: rows={} size={}\n".format(
          m.group(1), stats[0], stats[1]
        )
      fd.write(new_line)
