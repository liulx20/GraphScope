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

#ifndef RUNTIME_EXECUTE_OPS_SINK_H_
#define RUNTIME_EXECUTE_OPS_SINK_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

class SinkOpr : public IReadOperator {
 public:
  SinkOpr(const std::vector<int>& tag_ids) : tag_ids_(tag_ids) {}

  Context Eval(const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params, Context&& ctx,
               OprTimer& timer) override {
    ctx.tag_ids = tag_ids_;
    // gs::runtime::eval_sink_encoder(ctx, graph, tag_ids_);
    return ctx;
  }

 private:
  std::vector<int> tag_ids_;
};

class SinkOprBuilder : public IReadOperatorBuilder {
 public:
  SinkOprBuilder() = default;
  ~SinkOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    auto& opr = plan.plan(op_idx).opr().sink();
    std::vector<int> tag_ids;
    for (auto& tag : opr.tags()) {
      tag_ids.push_back(tag.tag().value());
    }
    return std::make_pair(std::make_unique<SinkOpr>(tag_ids), ctx_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kSink};
  }
};

class SinkInsertOpr : public IInsertOperator {
 public:
  SinkInsertOpr() {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    graph.Commit();
    return ctx;
  }
};

class SinkInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  SinkInsertOprBuilder() = default;
  ~SinkInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const physical::PhysicalPlan& plan,
                                         int op_idx) override {
    return std::make_unique<SinkInsertOpr>();
  }

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSink;
  }
};

}  // namespace ops

std::pair<std::unique_ptr<IInsertOperator>, int> create_sink_insert_operator(
    const physical::PhysicalPlan& plan, int op_id);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_SINK_H_