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

#ifndef CIDER_FUNCTION_MURMURHASH_H
#define CIDER_FUNCTION_MURMURHASH_H

#include <cstdint>
#include "type/data/funcannotations.h"

extern "C" RUNTIME_EXPORT NEVER_INLINE RUNTIME_EXPORT uint32_t
MurmurHash1(const void* key, int len, const uint32_t seed);

extern "C" RUNTIME_EXPORT NEVER_INLINE RUNTIME_EXPORT uint64_t
MurmurHash64A(const void* key, int len, uint64_t seed);

extern "C" RUNTIME_EXPORT NEVER_INLINE RUNTIME_EXPORT uint32_t
MurmurHash3(const void* key, int len, const uint32_t seed);

#endif  // CIDER_FUNCTION_MURMURHASH_H
