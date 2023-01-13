/*
 * Copyright (c) 2022 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "tests/utils/CiderNextgenBenchmarkBase.h"

using namespace cider::test::util;

int global_row_num = 0;

class CiderFilterSequenceTestNG : public CiderNextgenBenchmarkBase {
 public:
  CiderFilterSequenceTestNG() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 BIGINT NOT NULL, col_3 FLOAT "
        "NOT NULL, col_4 DOUBLE NOT NULL)";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  global_row_num,
                                                  {"col_1", "col_2", "col_3", "col_4"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64)});
  }
};

TEST_F(CiderFilterSequenceTestNG, integerFilterTest) {
  benchSQL("SELECT col_1 FROM test WHERE col_1 < 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 > 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 = 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 <= 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 >= 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 <> 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 IS NULL");
  benchSQL("SELECT col_1 FROM test WHERE col_1 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestNG, constantComparion) {
  benchSQL("SELECT col_1 FROM test WHERE TRUE");
  benchSQL("SELECT col_1 FROM test WHERE FALSE");

  benchSQL("SELECT col_1 FROM test WHERE 2 = 2");
  benchSQL("SELECT col_1 FROM test WHERE 2 > 2");
  benchSQL("SELECT col_1 FROM test WHERE 2 <> 2");

  benchSQL("SELECT col_1 FROM test WHERE 2 = 3");
  benchSQL("SELECT col_1 FROM test WHERE 2 <= 3");
  benchSQL("SELECT col_1 FROM test WHERE 2 <> 3");

  benchSQL("SELECT col_1 FROM test WHERE 2 <= 3 AND 2 >= 1");
  benchSQL("SELECT col_1 FROM test WHERE 2 <= 3 OR 2 >= 1");

  benchSQL("SELECT col_1 FROM test WHERE 2 <= 3 AND col_1 <> 77");
  benchSQL("SELECT col_1 FROM test WHERE 2 = 3 OR col_1 <> 77");
}

TEST_F(CiderFilterSequenceTestNG, complexFilterExpressions) {
  benchSQL("SELECT col_1 FROM test WHERE col_1 - 10 <= 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 + 10 >= 77");

  benchSQL("SELECT col_1 FROM test WHERE col_1 * 2 < 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 / 2 > 77");
  // FIXME(jikunshang): substrait-java does not support % yet, pending.
  // benchSQL("SELECT col_1 FROM test WHERE col_1 % 2 = 1");
}

TEST_F(CiderFilterSequenceTestNG, bigintFilterTest) {
  benchSQL("SELECT col_2 FROM test WHERE col_2 < 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 > 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 = 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 <= 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 >= 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 <> 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 IS NULL");
  benchSQL("SELECT col_2 FROM test WHERE col_2 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestNG, floatFilterTest) {
  benchSQL("SELECT col_3 FROM test WHERE col_3 < 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 > 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 = 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 <= 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 >= 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 <> 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 IS NULL");
  benchSQL("SELECT col_3 FROM test WHERE col_3 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestNG, doubleFilterTest) {
  benchSQL("SELECT col_4 FROM test WHERE col_4 < 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 > 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 = 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 <= 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 >= 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 <> 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 IS NULL");
  benchSQL("SELECT col_4 FROM test WHERE col_4 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestNG, multiFilterWithOrTest) {
  GTEST_SKIP_("benchSQL method not supported yet.");
  benchSQL("SELECT col_1 FROM test WHERE col_1 > 50 or col_1 < 5");
  benchSQL("SELECT col_1 FROM test WHERE col_1 IS NULL or col_1 < 5");
  benchSQL("SELECT col_2 FROM test WHERE col_2 > 50 or col_2 < 5");
  benchSQL("SELECT col_2 FROM test WHERE col_2 IS NULL or col_2 < 5");
  benchSQL("SELECT col_3 FROM test WHERE col_3 > 50 or col_3 < 5");
  benchSQL("SELECT col_3 FROM test WHERE col_3 IS NULL or col_3 < 5");
  benchSQL("SELECT col_4 FROM test WHERE col_4 > 50 or col_4 < 5");
  benchSQL("SELECT col_4 FROM test WHERE col_4 IS NULL or col_4 < 5");
}

TEST_F(CiderFilterSequenceTestNG, multiFilterWithAndTest) {
  benchSQL("SELECT col_1 FROM test WHERE col_1 < 50 and col_1 > 5");
  benchSQL("SELECT col_4 FROM test WHERE col_1 IS NOT NULL and col_1 > 5");
  benchSQL("SELECT col_2 FROM test WHERE col_2 < 50 and col_2 > 5");
  benchSQL("SELECT col_4 FROM test WHERE col_2 IS NOT NULL and col_2 > 5");
  benchSQL("SELECT col_3 FROM test WHERE col_3 < 50 and col_3 > 5");
  benchSQL("SELECT col_4 FROM test WHERE col_3 IS NOT NULL and col_3 > 5");
  benchSQL("SELECT col_4 FROM test WHERE col_4 < 50 and col_4 > 5");
  benchSQL("SELECT col_4 FROM test WHERE col_4 IS NOT NULL and col_4 > 5");
}

TEST_F(CiderFilterSequenceTestNG, multiColEqualTest) {
  benchSQL("SELECT col_1, col_2 FROM test WHERE col_1 = col_2");
  benchSQL("SELECT col_2, col_3 FROM test WHERE col_2 = col_3");
  benchSQL("SELECT col_2, col_4 FROM test WHERE col_2 = col_4");
  benchSQL("SELECT col_3, col_4 FROM test WHERE col_3 = col_4");
}

class CiderFilterRandomTestNG : public CiderNextgenBenchmarkBase {
 public:
  CiderFilterRandomTestNG() {
    table_name_ = "test";
    // FIXME: (jikunshang) revert string columns when supported to gen duck value.
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE,
           col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE, col_9 VARCHAR(10),
           col_10 VARCHAR(10));)";

    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  99,
                                                  {"col_1",
                                                   "col_2",
                                                   "col_3",
                                                   "col_4",
                                                   "col_5",
                                                   "col_6",
                                                   "col_7",
                                                   "col_8",
                                                   "col_9",
                                                   "col_10"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar)},
                                                  {2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                                                  GeneratePattern::Random,
                                                  1,
                                                  100000);
  }
};

TEST_F(CiderFilterRandomTestNG, inTest) {
  benchSQL("SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_1 in (24, 25, 26)",
           "in_int32_array.json");
  benchSQL("SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_2 in (24, 25, 26)",
           "in_int64_array.json");
  benchSQL("SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_3 in (24, 25, 26)",
           "in_fp32_array.json");
  benchSQL("SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_4 in (24, 25, 26)",
           "in_fp64_array.json");
  benchSQL("SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_3 not in (24, 25, 26)",
           "not_in_fp32_array.json");
  benchSQL("SELECT * FROM test WHERE col_1 IS NOT NULL AND col_1 in (24, 25, 26)");
  benchSQL("SELECT * FROM test WHERE col_2 IS NOT NULL AND col_2 in (24, 25, 26)");
  benchSQL("SELECT * FROM test WHERE col_3 IS NOT NULL AND col_3 in (24, 25, 26)");
  benchSQL("SELECT* FROM test WHERE col_4 IS NOT NULL AND col_4 in (24, 25, 26) ");
  benchSQL("SELECT * FROM test WHERE col_1 in (24, 25, 26) and col_2 > 20");
  benchSQL("SELECT * FROM test WHERE col_1 in (24 * 2 + 2, (25 + 2) * 10, 26)");
}
TEST_F(CiderFilterRandomTestNG, multiColRandomTest) {
  benchSQL("SELECT col_1, col_5 FROM test WHERE col_1 < col_5");
  benchSQL("SELECT col_2, col_6 FROM test WHERE col_2 < col_6");
  benchSQL("SELECT col_3, col_7 FROM test WHERE col_3 <= col_7");
  benchSQL("SELECT col_4, col_8 FROM test WHERE col_4 <= col_8");
  benchSQL("SELECT col_1, col_5 FROM test WHERE col_1 <> col_5");
  benchSQL(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_2 <= col_3 and col_2 >= "
      "col_1");
  benchSQL("SELECT col_1, col_2, col_3 FROM test WHERE col_2 >= col_3 or col_2 <= col_1",
           "");
  benchSQL("SELECT col_1, col_5 FROM test WHERE col_1 < col_5 AND col_5 > 0");
}

TEST_F(CiderFilterRandomTestNG, integerNullFilterTest) {
  benchSQL("SELECT col_1 FROM test WHERE col_1 < 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 > 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 <= 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 >= 77");
  benchSQL("SELECT col_1 FROM test WHERE col_1 IS NOT NULL AND col_1 < 77");
  benchSQL("SELECT col_2 FROM test WHERE col_2 IS NOT NULL AND col_2 > 77");
  benchSQL("SELECT col_3 FROM test WHERE col_3 IS NOT NULL AND col_3 <= 77");
  benchSQL("SELECT col_4 FROM test WHERE col_4 IS NOT NULL AND col_4 >= 77");
}

TEST_F(CiderFilterRandomTestNG, DistinctFromTest) {
  GTEST_SKIP_("benchSQL method not supported yet.");
  // IS DISTINCT FROM
  benchSQL(
      "SELECT * FROM test WHERE col_3 IS DISTINCT FROM col_7 OR col_4 IS DISTINCT FROM "
      "col_8",
      "is_distinct_from.json");
  // IS NOT DISTINCT FROM
  benchSQL(
      "SELECT * FROM test WHERE col_2 IS NOT DISTINCT FROM col_6 OR col_1 IS NOT "
      "DISTINCT FROM col_5",
      "is_not_distinct_from.json");
  // mixed case
  benchSQL(
      "SELECT * FROM test WHERE col_3 IS DISTINCT FROM col_7 OR col_1 IS NOT DISTINCT "
      "FROM col_5",
      "mixed_distinct_from.json");
  // mixed case with string

  GTEST_SKIP_("String is not supportted in nextgen.");
  benchSQL(
      "SELECT * FROM test WHERE col_9 IS DISTINCT FROM col_10 OR col_10 IS NOT DISTINCT "
      "FROM col_9",
      "mixed_distinct_from_string.json");
}

TEST_F(CiderFilterRandomTestNG, complexFilter) {
  GTEST_SKIP_("benchSQL method not supported yet.");
  benchSQL(
      "SELECT * FROM test WHERE (col_1 > 0 AND col_2 < 0) OR (col_1 < 0 AND col_2 > 0)");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // std::vector<int> row_nums{100'000, 1'000'000, 10'000'000};
  std::vector<int> row_nums{10'000};

  int err{0};

  for (int i = 0; i < row_nums.size(); i++) {
    global_row_num = row_nums[i];
    try {
      err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
  return err;
}
