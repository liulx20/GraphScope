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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/group_by.h"

namespace gs {
namespace runtime {
namespace ops {
class GroupByOpr : public IReadOperator {
 public:
  GroupByOpr(const physical::GroupBy& opr) : opr_(opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_group_by(opr_, graph, std::move(ctx));
  }

 private:
  physical::GroupBy opr_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> GroupByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int mappings_num = plan.plan(op_idx).opr().group_by().mappings_size();
  int func_num = plan.plan(op_idx).opr().group_by().functions_size();
  ContextMeta meta;
  for (int i = 0; i < mappings_num; ++i) {
    auto& key = plan.plan(op_idx).opr().group_by().mappings(i);
    if (key.has_alias()) {
      meta.set(key.alias().value());
    } else {
      meta.set(-1);
    }
  }
  for (int i = 0; i < func_num; ++i) {
    auto& func = plan.plan(op_idx).opr().group_by().functions(i);
    if (func.has_alias()) {
      meta.set(func.alias().value());
    } else {
      meta.set(-1);
    }
  }
  return std::make_pair(
      std::make_unique<GroupByOpr>(plan.plan(op_idx).opr().group_by()), meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs