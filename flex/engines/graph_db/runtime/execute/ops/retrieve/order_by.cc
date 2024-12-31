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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/order_by.h"

namespace gs {
namespace runtime {
namespace ops {
class OrderByOpr : public IReadOperator {
 public:
  OrderByOpr(const algebra::OrderBy& opr) : opr_(opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_order_by(opr_, graph, std::move(ctx), timer);
  }

 private:
  algebra::OrderBy opr_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> OrderByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(
      std::make_unique<OrderByOpr>(plan.plan(op_idx).opr().order_by()),
      ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs