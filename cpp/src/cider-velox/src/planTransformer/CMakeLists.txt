# Copyright(c) 2022-2023 Intel Corporation.
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

set(VELOX_PLAN_TRANSFORMER_SOURCES
    PlanNodeAddr.cpp
    PlanUtil.cpp
    PlanBranches.cpp
    PlanTransformer.cpp
    PlanPattern.cpp
    PlanRewriter.cpp
    SequencePlanPattern.cpp)

add_library(velox_plan_transformer ${VELOX_PLAN_TRANSFORMER_SOURCES})

function(ADD_VELOX_DEPENDENCY VELOX_DEP_LIB_NAME VELOX_DEP_LIB_PATH)
  if(NOT EXISTS ${VELOX_DEP_LIB_PATH})
    message(FATAL_ERROR "Velox library not exists: ${VELOX_DEP_LIB_PATH}")
  endif()
  set(VELOX_DEP_LIB facebook::velox::${VELOX_DEP_LIB_NAME})
  add_library(${VELOX_DEP_LIB} STATIC IMPORTED)
  set_target_properties(${VELOX_DEP_LIB} PROPERTIES IMPORTED_LOCATION
                                                    ${VELOX_DEP_LIB_PATH})
  target_link_libraries(velox_plan_transformer PUBLIC ${VELOX_DEP_LIB})
endfunction()

macro(ADD_VELOX_DEPENDENCIES)
  # add_velox_dependency(functions::sparksql::lib
  # "${VELOX_COMPONENTS_PATH}/functions/sparksql/libvelox_functions_spark.a")
  # add_velox_dependency(functions::sparksql::agg
  # "${VELOX_COMPONENTS_PATH}/functions/sparksql/aggregates/libvelox_functions_spark_aggregates.a")
  # add_velox_dependency(functions::prestosql::agg
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/aggregates/libvelox_aggregates.a")

  # add_velox_dependency(functions::prestosql::window
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/window/libvelox_window.a")
  # add_velox_dependency(velox::buffer
  # "${VELOX_COMPONENTS_PATH}/buffer/libvelox_buffer.a")

  add_velox_dependency(exec "${VELOX_COMPONENTS_PATH}/exec/libvelox_exec.a")
  # add_velox_dependency(substrait
  # "${VELOX_COMPONENTS_PATH}/substrait/libvelox_substrait_plan_converter.a")
  # add_velox_dependency(functions::isnull
  # "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_is_null_functions.a")
  # add_velox_dependency(functions::prestosql
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/registration/libvelox_functions_prestosql.a")
  # add_velox_dependency(functions::prestosql::impl
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/libvelox_functions_prestosql_impl.a")
  # add_velox_dependency(functions::json
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/json/libvelox_functions_json.a")
  # add_velox_dependency(functions::hyperloglog
  # "${VELOX_COMPONENTS_PATH}/common/hyperloglog/libvelox_common_hyperloglog.a")
  # add_velox_dependency(functions::lib
  # "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_lib.a")
  # add_velox_dependency(common::test_util
  # "${VELOX_COMPONENTS_PATH}/common/testutil/libvelox_test_util.a")
  # add_velox_dependency(parse::parser
  # "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_parser.a")
  # add_velox_dependency(parse::expression
  # "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_expression.a")
  # add_velox_dependency(vector::arrow::bridge
  # "${VELOX_COMPONENTS_PATH}/vector/arrow/libvelox_arrow_bridge.a")

  # add_velox_dependency(connector::hive
  # "${VELOX_COMPONENTS_PATH}/connectors/hive/libvelox_hive_connector.a")
  # add_velox_dependency(connector
  # "${VELOX_COMPONENTS_PATH}/connectors/libvelox_connector.a")
  # if(VELOX_ENABLE_HDFS) add_velox_dependency(connector::hive::hdfs
  # "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/hdfs/libvelox_hdfs.a")
  # endif() if(VELOX_ENABLE_S3) add_velox_dependency(connector::hive::s3fs
  # "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/s3fs/libvelox_s3fs.a")
  # endif() add_velox_dependency(dwio::dwrf::test_utils
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/test/utils/libvelox_dwrf_test_utils.a")
  # add_velox_dependency(dwio::dwrf::writer
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/writer/libvelox_dwio_dwrf_writer.a")
  # add_velox_dependency(dwio::dwrf::reader
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/reader/libvelox_dwio_dwrf_reader.a")
  # add_velox_dependency(dwio::dwrf::utils
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/utils/libvelox_dwio_dwrf_utils.a")
  # add_velox_dependency(dwio::dwrf::common
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/common/libvelox_dwio_dwrf_common.a")
  # add_velox_dependency(dwio::common::utils
  # "${VELOX_COMPONENTS_PATH}/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a")
  # add_velox_dependency(parquet
  # "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_reader.a")
  # add_velox_dependency(parquet::reader::native
  # "${VELOX_COMPONENTS_PATH}/dwio/parquet/reader/libvelox_dwio_native_parquet_reader.a")
  # add_velox_dependency(parquet::reader::thrift
  # "${VELOX_COMPONENTS_PATH}/dwio/parquet/thrift/libvelox_dwio_parquet_thrift.a")
  # add_velox_dependency(thrift
  # "${VELOX_BUILD_PATH}/third_party/arrow_ep/src/arrow_ep-build/thrift_ep-install/lib/libthrift.a")

  # add_velox_dependency(dwio::common
  # "${VELOX_COMPONENTS_PATH}/dwio/common/libvelox_dwio_common.a")
  # add_velox_dependency(functions::prestosql::types
  # "${VELOX_COMPONENTS_PATH}/functions/prestosql/types/libvelox_presto_types.a")
  # add_velox_dependency(expression
  # "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression.a")
  # add_velox_dependency(core "${VELOX_COMPONENTS_PATH}/core/libvelox_core.a")

  # add_velox_dependency(type "${VELOX_COMPONENTS_PATH}/type/libvelox_type.a")
  # add_velox_dependency(vector::serializes
  # "${VELOX_COMPONENTS_PATH}/serializers/libvelox_presto_serializer.a")
  # add_velox_dependency(functions::lib::util
  # "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_util.a")
  # add_velox_dependency(vector
  # "${VELOX_COMPONENTS_PATH}/vector/libvelox_vector.a")
  # add_velox_dependency(expression::function
  # "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression_functions.a")
  # add_velox_dependency(expression::type_calculation
  # "${VELOX_COMPONENTS_PATH}/expression/type_calculation/libvelox_type_calculation.a")

  # add_velox_dependency(common::caching
  # "${VELOX_COMPONENTS_PATH}/common/caching/libvelox_caching.a")
  # add_velox_dependency(common::base
  # "${VELOX_COMPONENTS_PATH}/common/base/libvelox_common_base.a")
  # add_velox_dependency(common::memory
  # "${VELOX_COMPONENTS_PATH}/common/memory/libvelox_memory.a")
  # add_velox_dependency(common::serialization
  # "${VELOX_COMPONENTS_PATH}/common/serialization/libvelox_serialization.a")
  # add_velox_dependency(common::base::exception
  # "${VELOX_COMPONENTS_PATH}/common/base/libvelox_exception.a")

  # add_velox_dependency(type::tz
  # "${VELOX_COMPONENTS_PATH}/type/tz/libvelox_type_tz.a")
  # add_velox_dependency(dwio::dwrf::proto
  # "${VELOX_COMPONENTS_PATH}/dwio/dwrf/proto/libvelox_dwio_dwrf_proto.a")
  # add_velox_dependency(dwio::common::exception
  # "${VELOX_COMPONENTS_PATH}/dwio/common/exception/libvelox_dwio_common_exception.a")
  # add_velox_dependency(dwio::common::encryption
  # "${VELOX_COMPONENTS_PATH}/dwio/common/encryption/libvelox_dwio_common_encryption.a")
  # add_velox_dependency(dwio::common::compression
  # "${VELOX_COMPONENTS_PATH}/dwio/common/compression/libvelox_dwio_common_compression.a")

  # add_velox_dependency(core::config
  # "${VELOX_COMPONENTS_PATH}/core/libvelox_config.a")
  # add_velox_dependency(common::encode
  # "${VELOX_COMPONENTS_PATH}/common/encode/libvelox_encode.a")
  # add_velox_dependency(common::time
  # "${VELOX_COMPONENTS_PATH}/common/time/libvelox_time.a")
  # add_velox_dependency(common::file
  # "${VELOX_COMPONENTS_PATH}/common/file/libvelox_file.a")
  # add_velox_dependency(common::process
  # "${VELOX_COMPONENTS_PATH}/common/process/libvelox_process.a")

  # add_velox_dependency(external::md5
  # "${VELOX_COMPONENTS_PATH}/external/md5/libmd5.a")
  # add_velox_dependency(external::date
  # "${VELOX_COMPONENTS_PATH}/external/date/libvelox_external_date.a")
  # if(BUILD_TESTS) add_velox_dependency(vector::test::util
  # "${VELOX_COMPONENTS_PATH}/vector/tests/utils/libvelox_vector_test_lib.a")
  # endif()
endmacro()

add_velox_dependencies()

# target_link_libraries(velox_plan_transformer velox_core)
