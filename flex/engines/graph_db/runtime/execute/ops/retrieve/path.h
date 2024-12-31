/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_PATH_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_PATH_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand.h"

namespace gs {

namespace runtime {

namespace ops {

class SPOrderByLimitOprBuilder : public IReadOperatorBuilder {
 public:
  SPOrderByLimitOprBuilder() = default;
  ~SPOrderByLimitOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;

  int stepping(int i) override { return i + 5; }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kPath,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
        physical::PhysicalOpr_Operator::OpKindCase::kSelect,
        physical::PhysicalOpr_Operator::OpKindCase::kProject,
        physical::PhysicalOpr_Operator::OpKindCase::kOrderBy,
    };
  }
};

class SPOprBuilder : public IReadOperatorBuilder {
 public:
  SPOprBuilder() = default;
  ~SPOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kPath,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
    };
  }
};

class PathExpandVOprBuilder : public IReadOperatorBuilder {
 public:
  PathExpandVOprBuilder() = default;
  ~PathExpandVOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kPath,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
    };
  }
};

class PathExpandOprBuilder : public IReadOperatorBuilder {
 public:
  PathExpandOprBuilder() = default;
  ~PathExpandOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;
  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kPath};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_PATH_H_