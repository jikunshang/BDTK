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

#pragma once

#include "planTransformer/PlanRewriter.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class TestCiderPlanNode : public PlanNode {
 public:
  TestCiderPlanNode(const PlanNodeId& id, std::shared_ptr<const PlanNode> source)
      : PlanNode(id), sources_{source} {}

  TestCiderPlanNode(const PlanNodeId& id,
                    std::vector<std::shared_ptr<const PlanNode>> source)
      : PlanNode(id), sources_(source) {}

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
};

class CiderPatternTestNodeRewriter : public PlanRewriter {
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithSingleSource(
      VeloxNodeAddrPlanSection& planSection,
      VeloxPlanNodeAddr& source) const override {
    VeloxPlanNodePtr testNodePtr =
        std::make_shared<TestCiderPlanNode>("CiderTest", source.nodePtr);
    return std::pair<bool, VeloxPlanNodePtr>(true, testNodePtr);
  };

  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithMultiSources(
      VeloxNodeAddrPlanSection& planSection,
      VeloxPlanNodeAddrList& srcList) const override {
    VeloxPlanNodeVec srcPtrList;
    for (VeloxPlanNodeAddr addr : srcList) {
      srcPtrList.emplace_back(addr.nodePtr);
    }
    auto resultPtr = std::make_shared<TestCiderPlanNode>("CiderJoin", srcPtrList);
    return std::pair<bool, VeloxPlanNodePtr>(true, resultPtr);
  };
};

}  // namespace facebook::velox::plugin::plantransformer::test
