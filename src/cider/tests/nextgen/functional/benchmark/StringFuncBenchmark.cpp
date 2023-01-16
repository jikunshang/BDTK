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


#define BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)       \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                             \
    ASSERT_FUNC("SELECT col_2 FROM test ");                                   \
    ASSERT_FUNC("SELECT col_1, col_2 FROM test ");                            \
    ASSERT_FUNC("SELECT * FROM test ");                                       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 = 'aaaa'");               \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 = '0000000000'");         \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 <> '0000000000'");        \
    ASSERT_FUNC("SELECT col_1 FROM test where col_2 <> '1111111111'");        \
    ASSERT_FUNC("SELECT col_1, col_2 FROM test where col_2 <> '2222222222'"); \
    ASSERT_FUNC("SELECT * FROM test where col_2 <> 'aaaaaaaaaaa'");           \
    ASSERT_FUNC("SELECT * FROM test where col_2 <> 'abcdefghijklmn'");        \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 IS NOT NULL");            \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 < 'uuu'");                \
  }

#define LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)                   \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '1111%'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111%'");                     \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1234%'");                     \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '22%22'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '_33%'");                       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '44_%'");                       \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666'");        \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888'");       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111'", "like_wo_cast.json"); \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 NOT LIKE '1111%'");                  \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444'");             \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE "        \
        "'%111%'");                                                                      \
  }

#define ESCAPE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)        \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                               \
    GTEST_SKIP_("Substrait does not support ESCAPE yet.");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%aaaa' ESCAPE '$' "); \
  }

#define IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)                    \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_2 IN ('0000000000', '1111111111', '2222222222')", \
        "in_string_array.json");                                                        \
    ASSERT_FUNC("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substr.json");                                  \
    ASSERT_FUNC("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substring.json");                               \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substr.json");                                            \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substring.json");                                         \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_1 >= 0 and SUBSTRING(col_2, 1, 4) IN "            \
        "('0000', '1111', '2222', '3333')",                                             \
        "in_string_nest_with_binop.json");                                              \
  }

#define BASIC_STRING_TEST_UNIT_BENCHMARK(TEST_CLASS, UNIT_NAME) \
  BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, benchSQL)

#define LIKE_STRING_TEST_UNIT_BENCHMARK(TEST_CLASS, UNIT_NAME) \
  LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, benchSQL)

#define IN_STRING_TEST_UNIT_BENCHMARK(TEST_CLASS, UNIT_NAME) \
  IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, benchSQL)


class CiderStringBenchmarkNextGen : public CiderNextgenBenchmarkBase {
 public:
  CiderStringBenchmarkNextGen() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL);)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        global_row_num,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

class CiderStringRandomBenchmarkNextGen : public CiderNextgenBenchmarkBase {
 public:
  CiderStringRandomBenchmarkNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        global_row_num,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Random,
        0,
        10);
  }
};

class CiderStringNullableBenchmarkNextGen : public CiderNextgenBenchmarkBase {
 public:
  CiderStringNullableBenchmarkNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER , col_2 VARCHAR(10) );)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        global_row_num,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

BASIC_STRING_TEST_UNIT_BENCHMARK(CiderStringBenchmarkNextGen, BasicStringTest)
LIKE_STRING_TEST_UNIT_BENCHMARK(CiderStringBenchmarkNextGen, LikeStringTest)
IN_STRING_TEST_UNIT_BENCHMARK(CiderStringBenchmarkNextGen, InStringTest)

BASIC_STRING_TEST_UNIT_BENCHMARK(CiderStringRandomBenchmarkNextGen, BasicRandomStringTest)
LIKE_STRING_TEST_UNIT_BENCHMARK(CiderStringRandomBenchmarkNextGen, LikeRandomStringTest)
IN_STRING_TEST_UNIT_BENCHMARK(CiderStringRandomBenchmarkNextGen, InRandomStringTest)

BASIC_STRING_TEST_UNIT_BENCHMARK(CiderStringNullableBenchmarkNextGen, BasicStringTest)
LIKE_STRING_TEST_UNIT_BENCHMARK(CiderStringNullableBenchmarkNextGen, LikeStringTest)
IN_STRING_TEST_UNIT_BENCHMARK(CiderStringNullableBenchmarkNextGen, InStringTest)

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  std::vector<int> row_nums{100'000, 1'000'000, 10'000'000};

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
