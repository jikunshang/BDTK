/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef CIDER_FUNCTION_STRING_LIKE_H
#define CIDER_FUNCTION_STRING_LIKE_H

#include "type/data/funcannotations.h"

#include <cstdint>

/*
 * @brief string_like performs the SQL LIKE and ILIKE operation
 * @param str string argument to be matched against pattern.  single-byte
 * character set only for now. null-termination not required.
 * @param str_len length of str
 * @param pattern pattern string for SQL LIKE
 * @param pat_len length of pattern
 * @param escape_char the escape character.  '\\' is expected by default.
 * @param is_ilike true if it is ILIKE, i.e., case-insensitive matching
 * @return true if str matchs pattern, false otherwise.  error condition
 * not handled for now.
 */
extern "C" RUNTIME_EXPORT bool string_like(const char* str,
                                           int str_len,
                                           const char* pattern,
                                           int pat_len,
                                           char escape_char);

extern "C" RUNTIME_EXPORT bool string_ilike(const char* str,
                                            int str_len,
                                            const char* pattern,
                                            int pat_len,
                                            char escape_char);

extern "C" RUNTIME_EXPORT bool string_like_simple(const char* str,
                                                  const int32_t str_len,
                                                  const char* pattern,
                                                  const int32_t pat_len);

extern "C" RUNTIME_EXPORT bool string_ilike_simple(const char* str,
                                                   const int32_t str_len,
                                                   const char* pattern,
                                                   const int32_t pat_len);

extern "C" RUNTIME_EXPORT bool string_lt(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT bool string_le(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT bool string_eq(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT bool string_ne(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT bool string_ge(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT bool string_gt(const char* lhs,
                                         const int32_t lhs_len,
                                         const char* rhs,
                                         const int32_t rhs_len);

extern "C" RUNTIME_EXPORT int32_t StringCompare(const char* s1,
                                                const int32_t s1_len,
                                                const char* s2,
                                                const int32_t s2_len);

#endif  // CIDER_FUNCTION_STRING_LIKE_H
