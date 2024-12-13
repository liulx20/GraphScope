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

#ifndef RUNTIME_EXECUTE_OPS_SCAN_H_
#define RUNTIME_EXECUTE_OPS_SCAN_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

class ScanOpr : public IReadOperator {
 public:
  ScanOpr(const gs::Schema& schema, const physical::Scan& scan_opr)
      : scan_opr_(scan_opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_scan(scan_opr_, graph, params, timer);
  }

 private:
  physical::Scan scan_opr_;
};

class ScanOprBuilder : public IReadOperatorBuilder {
 public:
  ScanOprBuilder() = default;
  ~ScanOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    ContextMeta ret_meta;
    int alias = -1;
    if (plan.plan(op_idx).opr().scan().has_alias()) {
      alias = plan.plan(op_idx).opr().scan().alias().value();
    }
    ret_meta.set(alias);
    return std::make_pair(
        std::make_unique<ScanOpr>(schema, plan.plan(op_idx).opr().scan()),
        ret_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kScan};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_SCAN_H_