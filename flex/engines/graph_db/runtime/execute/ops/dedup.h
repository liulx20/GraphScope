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

#ifndef RUNTIME_EXECUTE_OPS_DEDUP_H_
#define RUNTIME_EXECUTE_OPS_DEDUP_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

class DedupOpr : public IReadOperator {
 public:
  DedupOpr(const algebra::Dedup& opr) : opr_(opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_dedup(opr_, graph, std::move(ctx));
  }

  algebra::Dedup opr_;
};

class DedupOprBuilder : public IReadOperatorBuilder {
 public:
  DedupOprBuilder() = default;
  ~DedupOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    return std::make_pair(
        std::make_unique<DedupOpr>(plan.plan(op_idx).opr().dedup()), ctx_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kDedup};
  }
};

class DedupInsertOpr : public IInsertOperator {
 public:
  DedupInsertOpr(const algebra::Dedup& opr) : opr_(opr) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_dedup(opr_, graph, std::move(ctx));
  }

  algebra::Dedup opr_;
};

class DedupInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  DedupInsertOprBuilder() = default;
  ~DedupInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const physical::PhysicalPlan& plan,
                                         int op_id) override {
    return std::make_unique<DedupInsertOpr>(plan.plan(op_id).opr().dedup());
  }

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kDedup;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_DEDUP_H_