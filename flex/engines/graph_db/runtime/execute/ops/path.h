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

#ifndef RUNTIME_EXECUTE_OPS_PATH_H_
#define RUNTIME_EXECUTE_OPS_PATH_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

namespace ops {

static bool is_shortest_path_with_order_by_limit(
    const physical::PhysicalPlan& plan, int i, int& path_len_alias,
    int& vertex_alias, int& limit_upper) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  int start_tag = opr.path().start_tag().value();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 4 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    const auto& select_opr = plan.plan(i + 3).opr();
    const auto& project_opr = plan.plan(i + 4).opr();
    const auto& order_by_opr = plan.plan(i + 5).opr();
    if (!get_v_opr.has_vertex() || !get_v_filter_opr.has_vertex() ||
        !project_opr.has_project() || !order_by_opr.has_order_by()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }

    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }
    if (!select_opr.has_select()) {
      return false;
    }
    if (!select_opr.select().has_predicate()) {
      return false;
    }
    auto pred = select_opr.select().predicate();
    if (pred.operators_size() != 3) {
      return false;
    }
    if (!pred.operators(0).has_var() ||
        !(pred.operators(1).item_case() == common::ExprOpr::kLogical) ||
        pred.operators(1).logical() != common::Logical::NE ||
        !pred.operators(2).has_var()) {
      return false;
    }

    if (!pred.operators(0).var().has_tag() ||
        !pred.operators(2).var().has_tag()) {
      return false;
    }
    if (pred.operators(0).var().tag().id() != get_v_alias &&
        pred.operators(2).var().tag().id() != get_v_alias) {
      return false;
    }

    if (pred.operators(0).var().tag().id() != start_tag &&
        pred.operators(2).var().tag().id() != start_tag) {
      return false;
    }

    // only vertex and length(path)
    if (project_opr.project().mappings_size() != 2 ||
        project_opr.project().is_append()) {
      return false;
    }

    auto mapping = project_opr.project().mappings();
    if (!mapping[0].has_expr() || !mapping[1].has_expr()) {
      return false;
    }
    if (mapping[0].expr().operators_size() != 1 ||
        mapping[1].expr().operators_size() != 1) {
      return false;
    }
    if (!mapping[0].expr().operators(0).has_var() ||
        !mapping[1].expr().operators(0).has_var()) {
      return false;
    }
    if (!mapping[0].expr().operators(0).var().has_tag() ||
        !mapping[1].expr().operators(0).var().has_tag()) {
      return false;
    }
    common::Variable path_len_var0;
    common::Variable vertex_var;
    if (mapping[0].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[0].expr().operators(0).var();
      vertex_var = mapping[1].expr().operators(0).var();
      path_len_alias = mapping[0].alias().value();
      vertex_alias = mapping[1].alias().value();

    } else if (mapping[1].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[1].expr().operators(0).var();
      vertex_var = mapping[0].expr().operators(0).var();
      path_len_alias = mapping[1].alias().value();
      vertex_alias = mapping[0].alias().value();
    } else {
      return false;
    }
    if (!path_len_var0.has_property() || !path_len_var0.property().has_len()) {
      return false;
    }

    if (vertex_var.has_property()) {
      return false;
    }

    // must has order by limit
    if (!order_by_opr.order_by().has_limit()) {
      return false;
    }
    limit_upper = order_by_opr.order_by().limit().upper();
    if (order_by_opr.order_by().pairs_size() < 0) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].has_key()) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].key().has_tag()) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].key().tag().id() != path_len_alias) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].order() !=
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC) {
      return false;
    }
    return true;
  }
  return false;
}

static bool is_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

static bool is_all_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ALL_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }

  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

class SPOrderByLimitOpr : public IReadOperator {
 public:
  SPOrderByLimitOpr(const physical::PathExpand& opr,
                    const physical::PhysicalOpr_MetaData& meta,
                    const physical::GetV& get_v_opr,
                    const algebra::OrderBy& order_by_opr, int v_alias,
                    int path_len_alias, int limit)
      : opr_(opr),
        meta_(meta),
        get_v_opr_(get_v_opr),
        order_by_opr_(order_by_opr),
        v_alias_(v_alias),
        path_len_alias_(path_len_alias),
        limit_(limit) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto ret = gs::runtime::eval_shortest_path_with_order_by_length_limit(
        opr_, graph, std::move(ctx), params, meta_, get_v_opr_, v_alias_,
        path_len_alias_, limit_);
    return gs::runtime::eval_order_by(order_by_opr_, graph, std::move(ret),
                                      timer);
  }

 private:
  physical::PathExpand opr_;
  physical::PhysicalOpr_MetaData meta_;
  physical::GetV get_v_opr_;
  algebra::OrderBy order_by_opr_;
  int v_alias_;
  int path_len_alias_;
  int limit_;
};

class SPOrderByLimitOprBuilder : public IReadOperatorBuilder {
 public:
  SPOrderByLimitOprBuilder() = default;
  ~SPOrderByLimitOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    const auto& opr = plan.plan(op_idx).opr().path();
    int path_len_alias = -1;
    int vertex_alias = -1;
    int limit_upper = -1;
    if (is_shortest_path_with_order_by_limit(plan, op_idx, path_len_alias,
                                             vertex_alias, limit_upper)) {
      ContextMeta ret_meta = ctx_meta;
      ret_meta.set(vertex_alias);
      ret_meta.set(path_len_alias);
      return std::make_pair(std::make_unique<SPOrderByLimitOpr>(
                                opr, plan.plan(op_idx).meta_data(0),
                                plan.plan(op_idx + 2).opr().vertex(),
                                plan.plan(op_idx + 5).opr().order_by(),
                                vertex_alias, path_len_alias, limit_upper),
                            ret_meta);
    } else {
      return std::make_pair(nullptr, ContextMeta());
    }
  }

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

class SPOpr : public IReadOperator {
 public:
  SPOpr(const physical::PathExpand& opr,
        const physical::PhysicalOpr_MetaData& meta,
        const physical::GetV& get_v_opr, int v_alias)
      : opr_(opr), meta_(meta), get_v_opr_(get_v_opr), v_alias_(v_alias) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_shortest_path(opr_, graph, std::move(ctx), params,
                                           timer, meta_, get_v_opr_, v_alias_);
  }

 private:
  physical::PathExpand opr_;
  physical::PhysicalOpr_MetaData meta_;
  physical::GetV get_v_opr_;
  int v_alias_;
};

class ASPOpr : public IReadOperator {
 public:
  ASPOpr(const physical::PathExpand& opr,
         const physical::PhysicalOpr_MetaData& meta,
         const physical::GetV& get_v_opr, int v_alias)
      : opr_(opr), meta_(meta), get_v_opr_(get_v_opr), v_alias_(v_alias) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_all_shortest_paths(
        opr_, graph, std::move(ctx), params, meta_, get_v_opr_, v_alias_);
  }

 private:
  physical::PathExpand opr_;
  physical::PhysicalOpr_MetaData meta_;
  physical::GetV get_v_opr_;
  int v_alias_;
};

class SPOprBuilder : public IReadOperatorBuilder {
 public:
  SPOprBuilder() = default;
  ~SPOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    ContextMeta ret_meta = ctx_meta;
    if (is_shortest_path(plan, op_idx)) {
      auto vertex = plan.plan(op_idx + 2).opr().vertex();
      int v_alias = -1;
      if (!vertex.has_alias()) {
        v_alias = plan.plan(op_idx + 1).opr().vertex().has_alias()
                      ? plan.plan(op_idx + 1).opr().vertex().alias().value()
                      : -1;
      } else {
        v_alias = vertex.alias().value();
      }
      int alias = -1;
      if (plan.plan(op_idx).opr().path().has_alias()) {
        alias = plan.plan(op_idx).opr().path().alias().value();
      }
      ret_meta.set(v_alias);
      ret_meta.set(alias);
      return std::make_pair(
          std::make_unique<SPOpr>(plan.plan(op_idx).opr().path(),
                                  plan.plan(op_idx).meta_data(0), vertex,
                                  v_alias),
          ret_meta);
    } else if (is_all_shortest_path(plan, op_idx)) {
      auto vertex = plan.plan(op_idx + 2).opr().vertex();
      int v_alias = -1;
      if (!vertex.has_alias()) {
        v_alias = plan.plan(op_idx + 1).opr().vertex().has_alias()
                      ? plan.plan(op_idx + 1).opr().vertex().alias().value()
                      : -1;
      } else {
        v_alias = vertex.alias().value();
      }
      int alias = -1;
      if (plan.plan(op_idx).opr().path().has_alias()) {
        alias = plan.plan(op_idx).opr().path().alias().value();
      }
      ret_meta.set(v_alias);
      ret_meta.set(alias);
      return std::make_pair(
          std::make_unique<ASPOpr>(plan.plan(op_idx).opr().path(),
                                   plan.plan(op_idx).meta_data(0), vertex,
                                   v_alias),
          ret_meta);
    } else {
      return std::make_pair(nullptr, ContextMeta());
    }
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kPath,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
    };
  }
};

class PathExpandVOpr : public IReadOperator {
 public:
  PathExpandVOpr(const physical::PathExpand& opr,
                 const physical::PhysicalOpr_MetaData& meta, int v_alias)
      : opr_(opr), meta_(meta), v_alias_(v_alias) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_path_expand_v(opr_, graph, std::move(ctx), params,
                                           timer, meta_, v_alias_);
  }

 private:
  physical::PathExpand opr_;
  physical::PhysicalOpr_MetaData meta_;
  int v_alias_;
};

class PathExpandVOprBuilder : public IReadOperatorBuilder {
 public:
  PathExpandVOprBuilder() = default;
  ~PathExpandVOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    const auto& opr = plan.plan(op_idx).opr().path();
    const auto& next_opr = plan.plan(op_idx + 1).opr().vertex();
    if (opr.result_opt() ==
            physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
        opr.base().edge_expand().expand_opt() ==
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      int alias = -1;
      if (next_opr.has_alias()) {
        alias = next_opr.alias().value();
      }
      ContextMeta ret_meta = ctx_meta;
      ret_meta.set(alias);
      return std::make_pair(std::make_unique<PathExpandVOpr>(
                                opr, plan.plan(op_idx).meta_data(0), alias),
                            ret_meta);
    } else {
      return std::make_pair(nullptr, ContextMeta());
    }
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kPath,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
    };
  }
};

class PathExpandOpr : public IReadOperator {
 public:
  PathExpandOpr(const physical::PathExpand& opr,
                const physical::PhysicalOpr_MetaData& meta, int v_alias)
      : opr_(opr), meta_(meta), v_alias_(v_alias) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_path_expand_p(opr_, graph, std::move(ctx), params,
                                           meta_, v_alias_);
  }

 private:
  physical::PathExpand opr_;
  physical::PhysicalOpr_MetaData meta_;
  int v_alias_;
};

class PathExpandOprBuilder : public IReadOperatorBuilder {
 public:
  PathExpandOprBuilder() = default;
  ~PathExpandOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    const auto& opr = plan.plan(op_idx).opr().path();
    int alias = -1;
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    ContextMeta ret_meta = ctx_meta;
    ret_meta.set(alias);
    return std::make_pair(std::make_unique<PathExpandOpr>(
                              opr, plan.plan(op_idx).meta_data(0), alias),
                          ret_meta);
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kPath};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_PATH_H_