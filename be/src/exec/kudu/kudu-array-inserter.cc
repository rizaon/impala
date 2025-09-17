// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <limits>
#include <memory>

#include <kudu/client/client.h>
#include <kudu/client/write_op.h>

#include "testutil/gtest-util.h"
#include "util/kudu-status-util.h"

using kudu::Slice;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;

namespace impala {

// Utility program to facilitate complex column insertion into Kudu table.
// This script specifically tailored to insert into Kudu table
// impala::functional_kudu.kudu_array. See the detailed schema of
// functional_kudu.kudu_array table at functional_schema_template.sql.
// The destination table must be empty before this
// program run.

const char* KUDU_MASTER_DEFAULT_ADDR = "localhost:7051"; // Same as in tests/conftest.py
const char* KUDU_TEST_TABLE_NAME = "impala::functional_kudu.kudu_array";

constexpr auto INT32_ARRAY = {
    std::numeric_limits<int32_t>::lowest(), -1, std::numeric_limits<int32_t>::max()};
constexpr auto TIMESTAMP_ARRAY = {
    -17987443200000000, // See MIN_DATE_AS_UNIX_TIME in be/src/runtime/timestamp-test.cc
    -1L,
    253402300799999999, // See MAX_DATE_AS_UNIX_TIME in be/src/runtime/timestamp-test.cc
};
const vector<Slice> UTF8_ARRAY = {u8"Σ", u8"π", u8"λ"};
constexpr auto DECIMAL18_ARRAY = {
    -999'999'999'999'999'999, // 18 digits
    -1L,
    999'999'999'999'999'999, // 18 digits
};

// 'id' starts from 0, same as Python's range().
int id = 0;

void KuduInsertNulls(shared_ptr<KuduSession> session, shared_ptr<KuduTable> table) {
  std::unique_ptr<KuduInsert> insert(table->NewInsert());
  KUDU_ASSERT_OK(insert->mutable_row()->SetInt8("id", id));
  KUDU_ASSERT_OK(insert->mutable_row()->SetNull("array_int"));
  KUDU_ASSERT_OK(insert->mutable_row()->SetNull("array_timestamp"));
  KUDU_ASSERT_OK(insert->mutable_row()->SetNull("array_varchar"));
  KUDU_ASSERT_OK(insert->mutable_row()->SetNull("array_decimal"));
  KUDU_ASSERT_OK(session->Apply(insert.release()));
  ++id;
}

void KuduInsertArrays(shared_ptr<KuduSession> session, shared_ptr<KuduTable> table,
    const vector<bool>& non_null) {
  std::unique_ptr<KuduInsert> insert(table->NewInsert());
  KUDU_ASSERT_OK(insert->mutable_row()->SetInt8("id", id));
  KUDU_ASSERT_OK(insert->mutable_row()->SetArrayInt32(
      "array_int", non_null.empty() ? vector<int32_t>() : INT32_ARRAY, non_null));
  KUDU_ASSERT_OK(insert->mutable_row()->SetArrayUnixTimeMicros("array_timestamp",
      non_null.empty() ? vector<int64_t>() : TIMESTAMP_ARRAY, non_null));
  KUDU_ASSERT_OK(insert->mutable_row()->SetArrayVarchar(
      "array_varchar", non_null.empty() ? vector<Slice>() : UTF8_ARRAY, non_null));
  KUDU_ASSERT_OK(insert->mutable_row()->SetArrayUnscaledDecimal(
      "array_decimal", non_null.empty() ? vector<int64_t>() : DECIMAL18_ARRAY, non_null));
  KUDU_ASSERT_OK(session->Apply(insert.release()));
  ++id;
}

void RunKuduArrayInsert() {
  shared_ptr<KuduClient> client;
  // Connect to the cluster.
  KUDU_ASSERT_OK(KuduClientBuilder()
          .add_master_server_addr(KUDU_MASTER_DEFAULT_ADDR)
          .Build(&client));
  ASSERT_NE(KUDU_TEST_TABLE_NAME, nullptr);
  shared_ptr<KuduTable> table;
  KUDU_ASSERT_OK(client->OpenTable(KUDU_TEST_TABLE_NAME, &table));

  shared_ptr<KuduSession> session = client->NewSession();
  KUDU_ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // The array slot is NULL.
  KuduInsertNulls(session, table);

  // The array is not empty and no element is NULL.
  KuduInsertArrays(session, table, {true, true, true});

  // The array is empty.
  KuduInsertArrays(session, table, {});

  // Array element at the start is NULL.
  KuduInsertArrays(session, table, {false, true, true});

  // Array element at the middle is NULL.
  KuduInsertArrays(session, table, {true, false, true});

  // Array element at the end is NULL.
  KuduInsertArrays(session, table, {true, true, false});

  KUDU_EXPECT_OK(session->Flush());
  vector<KuduError*> errors;
  bool overflowed;
  session->GetPendingErrors(&errors, &overflowed);
  EXPECT_FALSE(overflowed);
  for (auto error : errors) {
    KUDU_EXPECT_OK(error->status());
  }
  ASSERT_EQ(errors.size(), 0);
}
} // namespace impala

int main(int argc, char** argv) {
  impala::RunKuduArrayInsert();
  return 0;
}
