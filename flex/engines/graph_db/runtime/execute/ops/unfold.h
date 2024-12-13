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

#ifndef RUNTIME_EXECUTE_OPS_UNFOLD_H_
#define RUNTIME_EXECUTE_OPS_UNFOLD_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

class UnfoldOpr : public IReadOperator {
 public:
  UnfoldOpr(const physical::Unfold& opr) : opr_(opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_unfold(opr_, std::move(ctx));
  }

 private:
  physical::Unfold opr_;
};

class UnfoldOprBuilder : public IReadOperatorBuilder {
 public:
  UnfoldOprBuilder() = default;
  ~UnfoldOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    ContextMeta ret_meta = ctx_meta;
    int alias = plan.plan(op_idx).opr().unfold().alias().value();
    ret_meta.set(alias);
    return std::make_pair(
        std::make_unique<UnfoldOpr>(plan.plan(op_idx).opr().unfold()),
        ret_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kUnfold};
  }
};

class UnfoldInsertOpr : public IInsertOperator {
 public:
  UnfoldInsertOpr(const physical::Unfold& opr) : opr_(opr) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_unfold(opr_, std::move(ctx));
  }

 private:
  physical::Unfold opr_;
};

class UnfoldInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  UnfoldInsertOprBuilder() = default;
  ~UnfoldInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const physical::PhysicalPlan& plan,
                                         int op_id) override {
    return std::make_unique<UnfoldInsertOpr>(plan.plan(op_id).opr().unfold());
  }

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kUnfold;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_UNFOLD_H_