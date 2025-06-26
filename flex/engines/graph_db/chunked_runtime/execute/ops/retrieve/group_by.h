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

#ifndef CHUNKED_RUNTIME_EXECUTE_OPS_RETRIEVE_GROUP_BY_H_
#define CHUNKED_RUNTIME_EXECUTE_OPS_RETRIEVE_GROUP_BY_H_
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
class GroupByOprBuilder : public IReadOperatorBuilder {
 public:
  GroupByOprBuilder() = default;
  ~GroupByOprBuilder() = default;

  bl::result<ReadOpBuildResultT> Build(std::unique_ptr<IReadOpr> src_opr,
                                       const gs::Schema& schema,
                                       const ContextMeta& ctx_meta,
                                       const physical::PhysicalPlan& plan,
                                       int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kGroupBy};
  }
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif