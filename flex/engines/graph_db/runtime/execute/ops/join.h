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

#ifndef RUNTIME_EXECUTE_OPS_JOIN_H_
#define RUNTIME_EXECUTE_OPS_JOIN_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"
#include "flex/engines/graph_db/runtime/execute/pipeline.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

class RLJoinOpr : public IReadOperator {
 public:
  RLJoinOpr(gs::runtime::ReadPipeline&& left_pipeline,
            gs::runtime::ReadPipeline&& right_pipeline, int reuse_tag,
            int reuse_alias, const physical::Join& join_opr)
      : left_pipeline_(std::move(left_pipeline)),
        right_pipeline_(std::move(right_pipeline)),
        reuse_tag_(reuse_tag),
        reuse_alias_(reuse_alias),
        opr_(join_opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto left_ctx =
        left_pipeline_.Execute(graph, std::move(ctx), params, timer);
    std::vector<size_t> offset;
    left_ctx.get(reuse_tag_)->generate_dedup_offset(offset);

    gs::runtime::Context right_ctx;
    right_ctx.set(reuse_alias_, left_ctx.get(reuse_tag_));
    right_ctx.reshuffle(offset);

    right_ctx =
        right_pipeline_.Execute(graph, std::move(right_ctx), params, timer);
    return gs::runtime::eval_join(graph, params, opr_, std::move(left_ctx),
                                  std::move(right_ctx));
  }

 private:
  gs::runtime::ReadPipeline left_pipeline_;
  gs::runtime::ReadPipeline right_pipeline_;
  int reuse_tag_;
  int reuse_alias_;

  physical::Join opr_;
};

class JoinOpr : public IReadOperator {
 public:
  JoinOpr(gs::runtime::ReadPipeline&& left_pipeline,
          gs::runtime::ReadPipeline&& right_pipeline,
          const physical::Join& join_opr)
      : left_pipeline_(std::move(left_pipeline)),
        right_pipeline_(std::move(right_pipeline)),
        opr_(join_opr) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    gs::runtime::Context ret_dup(ctx);

    auto left_ctx =
        left_pipeline_.Execute(graph, std::move(ctx), params, timer);
    auto right_ctx =
        right_pipeline_.Execute(graph, std::move(ret_dup), params, timer);

    return gs::runtime::eval_join(graph, params, opr_, std::move(left_ctx),
                                  std::move(right_ctx));
  }

 private:
  gs::runtime::ReadPipeline left_pipeline_;
  gs::runtime::ReadPipeline right_pipeline_;

  physical::Join opr_;
};

static bool try_reuse_left_plan_column(const physical::Join& op, int& Tag,
                                       int& Alias) {
  int left_key0 = op.left_keys(0).tag().id();
  int left_key1 = op.left_keys(1).tag().id();
  int right_key0 = op.right_keys(0).tag().id();
  int right_key1 = op.right_keys(1).tag().id();
  if (!op.right_keys(0).has_node_type() || !op.right_keys(1).has_node_type()) {
    return false;
  }
  if (op.right_keys(0).node_type().type_case() !=
          common::IrDataType::kGraphType ||
      op.right_keys(1).node_type().type_case() !=
          common::IrDataType::kGraphType) {
    return false;
  }

  if ((op.right_keys(0).node_type().graph_type().element_opt() !=
       common::GraphDataType::GraphElementOpt::
           GraphDataType_GraphElementOpt_VERTEX) ||
      (op.right_keys(1).node_type().graph_type().element_opt() !=
       common::GraphDataType::GraphElementOpt::
           GraphDataType_GraphElementOpt_VERTEX)) {
    return false;
  }
  if (op.right_keys(1).node_type().graph_type().graph_data_type_size() != 1 ||
      op.right_keys(0).node_type().graph_type().graph_data_type_size() != 1) {
    return false;
  }
  if (op.right_keys(0)
          .node_type()
          .graph_type()
          .graph_data_type(0)
          .label()
          .label() != op.right_keys(1)
                          .node_type()
                          .graph_type()
                          .graph_data_type(0)
                          .label()
                          .label()) {
    return false;
  }
  auto right_plan = op.right_plan();

  if (right_plan.plan(0).opr().has_scan()) {
    auto scan = right_plan.plan(0).opr().scan();
    int alias = -1;
    if (scan.has_alias()) {
      alias = scan.alias().value();
    }
    if (!(alias == right_key0 || alias == right_key1)) {
      return false;
    }
    if (alias == right_key0) {
      Tag = left_key0;
      Alias = alias;
    } else {
      Tag = left_key1;
      Alias = alias;
    }

    if (scan.has_idx_predicate()) {
      return false;
    }
    auto params = scan.params();
    if (params.has_predicate()) {
      return false;
    }
    if (params.tables_size() != 1) {
      return false;
    }
    int num = right_plan.plan().size();
    auto last_op = right_plan.plan(num - 1);

    if (last_op.opr().has_edge()) {
      auto edge = last_op.opr().edge();
      int alias = -1;
      if (edge.has_alias()) {
        alias = edge.alias().value();
      }
      if (alias != right_key0 && alias != right_key1) {
        return false;
      }
      if (edge.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
        return false;
      }
      if (!edge.has_params()) {
        return false;
      }
      if (edge.params().tables_size() != 1) {
        return false;
      }

      if (edge.params().has_predicate()) {
        return false;
      }
      return true;
    }
  }
  return false;
}

class JoinOprBuilder : public IReadOperatorBuilder {
 public:
  JoinOprBuilder() = default;
  ~JoinOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    int tag = -1;
    int alias = -1;

    ContextMeta ret_meta;
    auto& right_keys = plan.plan(op_idx).opr().join().right_keys();
    int left_keys_size = plan.plan(op_idx).opr().join().left_keys_size();
    int right_keys_size = plan.plan(op_idx).opr().join().right_keys_size();
    std::vector<int> right_columns;
    for (int i = 0; i < right_keys.size(); ++i) {
      right_columns.push_back(right_keys.Get(i).tag().id());
    }
    auto join_kind = plan.plan(op_idx).opr().join().join_kind();

    if (left_keys_size == 2 && right_keys_size == 2) {
      if (try_reuse_left_plan_column(plan.plan(op_idx).opr().join(), tag,
                                     alias)) {
        auto pair1 = PlanParser::get().parse_read_pipeline_with_meta(
            schema, ctx_meta, plan.plan(op_idx).opr().join().left_plan(), 0);
        ContextMeta right_meta;
        right_meta.set(alias);
        auto pair2 = PlanParser::get().parse_read_pipeline_with_meta(
            schema, right_meta, plan.plan(op_idx).opr().join().right_plan(), 1);

        auto& ctx_meta1 = pair1.second;
        auto& ctx_meta2 = pair2.second;

        if (join_kind == physical::Join_JoinKind::Join_JoinKind_SEMI ||
            join_kind == physical::Join_JoinKind::Join_JoinKind_ANTI) {
          ret_meta = ctx_meta1;
        } else if (join_kind == physical::Join_JoinKind::Join_JoinKind_INNER) {
          ret_meta = ctx_meta1;
          for (auto k : ctx_meta2.columns()) {
            ret_meta.set(k);
          }
        } else {
          CHECK(join_kind == physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER);
          ret_meta = ctx_meta1;
          for (auto k : ctx_meta2.columns()) {
            if (std::find(right_columns.begin(), right_columns.end(), k) ==
                right_columns.end()) {
              ret_meta.set(k);
            }
          }
        }

        return std::make_pair(
            std::make_unique<RLJoinOpr>(std::move(pair1.first),
                                        std::move(pair2.first), tag, alias,
                                        plan.plan(op_idx).opr().join()),
            ret_meta);
      }
    }
    auto pair1 = PlanParser::get().parse_read_pipeline_with_meta(
        schema, ctx_meta, plan.plan(op_idx).opr().join().left_plan(), 0);
    auto pair2 = PlanParser::get().parse_read_pipeline_with_meta(
        schema, ctx_meta, plan.plan(op_idx).opr().join().right_plan(), 0);
    auto& ctx_meta1 = pair1.second;
    auto& ctx_meta2 = pair2.second;
    if (join_kind == physical::Join_JoinKind::Join_JoinKind_SEMI ||
        join_kind == physical::Join_JoinKind::Join_JoinKind_ANTI) {
      ret_meta = ctx_meta1;
    } else if (join_kind == physical::Join_JoinKind::Join_JoinKind_INNER) {
      ret_meta = ctx_meta1;
      for (auto k : ctx_meta2.columns()) {
        ret_meta.set(k);
      }
    } else {
      CHECK(join_kind == physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER);
      ret_meta = ctx_meta1;
      for (auto k : ctx_meta2.columns()) {
        if (std::find(right_columns.begin(), right_columns.end(), k) ==
            right_columns.end()) {
          ret_meta.set(k);
        }
      }
    }
    return std::make_pair(std::make_unique<JoinOpr>(
                              std::move(pair1.first), std::move(pair2.first),
                              plan.plan(op_idx).opr().join()),
                          ret_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kJoin};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_JOIN_H_