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

class CiderStringFunctionBenchmark : public CiderNextgenBenchmarkBase {
 public:
  CiderStringFunctionBenchmark() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(20) NOT NULL, col_3 VARCHAR(20));)";

    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  100'000,
                                                  {"col_1", "col_2", "col_3"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar)},
                                                  {0, 0, 2},
                                                  GeneratePattern::Sequence,
                                                  10,
                                                  20);
  }
};

TEST_F(CiderStringFunctionBenchmark, substring) {
  benchSQL("SELECT SUBSTRING(col_2, 1, 5) FROM test ");
  benchSQL("SELECT SUBSTRING(col_3, 1, 5) FROM test ");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  std::vector<int> row_nums{100'000, 1'000'000, 10'000'000};

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
