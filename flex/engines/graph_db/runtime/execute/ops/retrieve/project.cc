
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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/project.h"
#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/project.h"

namespace gs {
namespace runtime {
namespace ops {

template <typename T>
struct ValueCollector {
  void collect(const Expr& expr, size_t idx) {
    auto val = expr.eval_path(idx);
    builder.push_back_opt(TypedConverter<T>::to_typed(val));
  }
  auto get(const Expr&) { return builder.finish(); }
  ValueColumnBuilder<T> builder;
};

template <typename T>
struct OptionalValueCollector {
  void collect(const Expr& expr, size_t idx) {
    auto val = expr.eval_path(idx, 0);
    if (val.is_null()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(TypedConverter<T>::to_typed(val), true);
    }
  }
  auto get(const Expr&) { return builder.finish(); }
  OptionalValueColumnBuilder<T> builder;
};

struct SLVertexCollector {
  SLVertexCollector(label_t v_label) : builder(v_label) {}
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx).as_vertex();
    builder.push_back_opt(v.vid_);
  }
  auto get(const Expr&) { return builder.finish(); }
  SLVertexColumnBuilder builder;
};

struct MLVertexCollector {
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx).as_vertex();
    builder.push_back_vertex(v);
  }
  auto get(const Expr&) { return builder.finish(); }
  MLVertexColumnBuilder builder;
};

struct EdgeCollector {
  void collect(const Expr& expr, size_t idx) {
    auto e = expr.eval_path(idx);
    builder.push_back_elem(e);
  }
  auto get(const Expr&) { return builder.finish(); }
  BDMLEdgeColumnBuilder builder;
};

struct ListCollector {
  ListCollector(const Expr& expr) : builder_(expr.builder()) {}
  void collect(const Expr& expr, size_t idx) {
    builder_->push_back_elem(expr.eval_path(idx));
  }
  auto get(const Expr& expr) {
    auto& list_builder = dynamic_cast<ListValueColumnBuilderBase&>(*builder_);
    if (!list_builder.impls_has_been_set()) {
      list_builder.set_list_impls(expr.get_list_impls());
    }
    return builder_->finish();
  }
  std::shared_ptr<IContextColumnBuilder> builder_;
};

struct TupleCollector {
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx);
    builder.push_back_elem(v);
  }
  auto get(const Expr&) { return builder.finish(); }
  ValueColumnBuilder<Tuple> builder;
};

struct OptionalTupleCollector {
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx, 0);
    if (v.is_null()) {
      builder.push_back_null();
    } else {
      builder.push_back_elem(v);
    }
  }
  auto get(const Expr&) { return builder.finish(); }
  OptionalValueColumnBuilder<Tuple> builder;
};

struct MapCollector {
  MapCollector(const Expr& expr) : builder(expr.builder()) {}
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx);
    builder->push_back_elem(v);
  }
  auto get(const Expr&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

struct OptionalMapCollector {
  OptionalMapCollector(const Expr& expr) : builder(expr.builder()) {}
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx, 0);
    builder->push_back_elem(v);
  }
  auto get(const Expr&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

struct StringArrayCollector {
  StringArrayCollector(const Expr& expr) : builder(expr.builder()) {}
  void collect(const Expr& expr, size_t idx) {
    auto v = expr.eval_path(idx);
    builder->push_back_elem(v);
  }
  auto get(const Expr&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

template <typename EXPR, typename RESULT_T>
struct CaseWhenCollector {
  CaseWhenCollector(RESULT_T then_value, RESULT_T else_value)
      : then_value(then_value), else_value(else_value) {}
  void collect(const EXPR& expr, size_t idx) {
    if (expr(idx)) {
      builder.push_back_opt(then_value);
    } else {
      builder.push_back_opt(else_value);
    }
  }
  auto get(const EXPR&) { return builder.finish(); }
  ValueColumnBuilder<RESULT_T> builder;
  RESULT_T then_value;
  RESULT_T else_value;
};

template <typename EXPR>
std::unique_ptr<ProjectExprBase> create_case_when_project(
    EXPR&& expr, const common::Value& then_value,
    const common::Value& else_value, int alias) {
  CHECK(then_value.item_case() == else_value.item_case());
  switch (then_value.item_case()) {
  case common::Value::kI32: {
    auto collector =
        CaseWhenCollector<EXPR, int32_t>(then_value.i32(), else_value.i32());
    return std::make_unique<ProjectExpr<EXPR, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case common::Value::kI64: {
    auto collector =
        CaseWhenCollector<EXPR, int64_t>(then_value.i64(), else_value.i64());
    return std::make_unique<ProjectExpr<EXPR, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case common::Value::kF64: {
    auto collector =
        CaseWhenCollector<EXPR, double>(then_value.f64(), else_value.f64());
    return std::make_unique<ProjectExpr<EXPR, decltype(collector)>>(
        std::move(expr), collector, alias);
  }

  default:
    LOG(ERROR) << "Unsupported type for case when collector";
    return nullptr;
  }
}

template <typename SP_PRED_T>
struct SPOpr {
  SPOpr(std::shared_ptr<IVertexColumn> vertex_col, SP_PRED_T&& pred)
      : vertex_col(vertex_col), pred(std::move(pred)) {}
  bool operator()(size_t idx) const {
    auto v = vertex_col->get_vertex(idx);
    return pred(v.label_, v.vid_);
  }
  std::shared_ptr<IVertexColumn> vertex_col;
  SP_PRED_T pred;
};

template <typename T>
static std::unique_ptr<ProjectExprBase> _make_project_expr(Expr&& expr,
                                                           int alias,
                                                           int row_num) {
  if (!expr.is_optional()) {
    ValueCollector<T> collector;
    collector.builder.reserve(row_num);
    return std::make_unique<ProjectExpr<Expr, ValueCollector<T>>>(
        std::move(expr), collector, alias);
  } else {
    OptionalValueCollector<T> collector;
    collector.builder.reserve(row_num);
    return std::make_unique<ProjectExpr<Expr, OptionalValueCollector<T>>>(
        std::move(expr), collector, alias);
  }
}
template <typename T>
static std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>
_make_project_expr(const common::Expression& expr, int alias) {
  return [=](const GraphReadInterface& graph,
             const std::map<std::string, std::string>& params,
             const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
    Expr e(graph, ctx, params, expr, VarType::kPathVar);
    size_t row_num = ctx.row_num();
    if (!e.is_optional()) {
      ValueCollector<T> collector;
      collector.builder.reserve(row_num);
      return std::make_unique<ProjectExpr<Expr, ValueCollector<T>>>(
          std::move(e), collector, alias);
    } else {
      OptionalValueCollector<T> collector;
      collector.builder.reserve(row_num);
      return std::make_unique<ProjectExpr<Expr, OptionalValueCollector<T>>>(
          std::move(e), collector, alias);
    }
  };
}

bool is_exchange_index(const common::Expression& expr, int alias, int& tag) {
  if (expr.operators().size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (var.has_property()) {
      return false;
    }

    if (var.has_tag()) {
      tag = var.tag().id();
    }
    // if (tag == alias) {
    return true;
    //}
  }
  return false;
}

bool is_check_property_in_range(const common::Expression& expr, int& tag,
                                std::string& name, std::string& lower,
                                std::string& upper, common::Value& then_value,
                                common::Value& else_value) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 7) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::GE) {
        return false;
      }
    }
    auto lower_param = when.operators(2);
    if (lower_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    lower = lower_param.param().name();
    {
      auto op = when.operators(3);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::AND) {
        return false;
      }
    }
    {
      if (!when.operators(4).has_var()) {
        return false;
      }
      auto var = when.operators(4).var();
      if (!var.has_tag()) {
        return false;
      }
      if (var.tag().id() != tag) {
        return false;
      }
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key() && name != var.property().key().name()) {
        return false;
      }
    }

    auto op = when.operators(5);
    if (op.item_case() != common::ExprOpr::kLogical ||
        op.logical() != common::LT) {
      return false;
    }
    auto upper_param = when.operators(6);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    upper = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_check_property_cmp(const common::Expression& expr, int& tag,
                           std::string& name, std::string& target,
                           common::Value& then_value, common::Value& else_value,
                           SPPredicateType& ptype) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 3) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical) {
        return false;
      }
      switch (op.logical()) {
      case common::LT:
        ptype = SPPredicateType::kPropertyLT;
        break;
      case common::LE:
        ptype = SPPredicateType::kPropertyLE;
        break;
      case common::GT:
        ptype = SPPredicateType::kPropertyGT;
        break;
      case common::GE:
        ptype = SPPredicateType::kPropertyGE;
        break;
      case common::EQ:
        ptype = SPPredicateType::kPropertyEQ;
        break;
      case common::NE:
        ptype = SPPredicateType::kPropertyNE;
        break;
      default:
        return false;
      }
    }
    auto upper_param = when.operators(2);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    target = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

template <typename T>
static std::unique_ptr<ProjectExprBase> create_sp_pred_case_when(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params,
    const std::shared_ptr<IVertexColumn>& vertex, SPPredicateType type,
    const std::string& name, const std::string& target,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (type == SPPredicateType::kPropertyLT) {
    SPOpr sp(vertex,
             VertexPropertyLTPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  } else if (type == SPPredicateType::kPropertyGT) {
    SPOpr sp(vertex,
             VertexPropertyGTPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  } else if (type == SPPredicateType::kPropertyLE) {
    SPOpr sp(vertex,
             VertexPropertyLEPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  } else if (type == SPPredicateType::kPropertyGE) {
    SPOpr sp(vertex,
             VertexPropertyGEPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  } else if (type == SPPredicateType::kPropertyEQ) {
    SPOpr sp(vertex,
             VertexPropertyEQPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  } else if (type == SPPredicateType::kPropertyNE) {
    SPOpr sp(vertex,
             VertexPropertyNEPredicateBeta<T>(graph, name, params.at(target)));
    return create_case_when_project(std::move(sp), then_value, else_value,
                                    alias);
  }
  return nullptr;
}

// in the case of data_type is not set, we need to infer the type from the
// expr
static std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>
make_project_expr(const common::Expression& expr, int alias) {
  return [=](const GraphReadInterface& graph,
             const std::map<std::string, std::string>& params,
             const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
    Expr e(graph, ctx, params, expr, VarType::kPathVar);

    switch (e.type()) {
    case RTAnyType::kI64Value: {
      return _make_project_expr<int64_t>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kStringValue: {
      return _make_project_expr<std::string_view>(std::move(e), alias,
                                                  ctx.row_num());
    } break;
    case RTAnyType::kDate32: {
      return _make_project_expr<Day>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kTimestamp: {
      return _make_project_expr<Date>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kVertex: {
      MLVertexCollector collector;
      collector.builder.reserve(ctx.row_num());
      return std::make_unique<ProjectExpr<Expr, MLVertexCollector>>(
          std::move(e), collector, alias);
    } break;
    case RTAnyType::kI32Value: {
      return _make_project_expr<int32_t>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kF64Value: {
      return _make_project_expr<double>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kEdge: {
      EdgeCollector collector;
      return std::make_unique<ProjectExpr<Expr, EdgeCollector>>(
          std::move(e), collector, alias);
    } break;
    case RTAnyType::kTuple: {
      if (e.is_optional()) {
        OptionalTupleCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<ProjectExpr<Expr, OptionalTupleCollector>>(
            std::move(e), collector, alias);
      } else {
        TupleCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<ProjectExpr<Expr, TupleCollector>>(
            std::move(e), collector, alias);
      }
    } break;
    case RTAnyType::kList: {
      ListCollector collector(e);
      return std::make_unique<ProjectExpr<Expr, ListCollector>>(
          std::move(e), collector, alias);
    } break;
    case RTAnyType::kMap: {
      if (!e.is_optional()) {
        MapCollector collector(e);
        return std::make_unique<ProjectExpr<Expr, MapCollector>>(
            std::move(e), collector, alias);
      } else {
        OptionalMapCollector collector(e);
        return std::make_unique<ProjectExpr<Expr, OptionalMapCollector>>(
            std::move(e), collector, alias);
      }
    } break;
    default:
      LOG(FATAL) << "not support - " << static_cast<int>(e.type());
      break;
    }
    return nullptr;
  };
}

static std::optional<std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>>
parse_special_expr(const common::Expression& expr, int alias) {
  int tag = -1;
  if (is_exchange_index(expr, alias, tag)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      return std::make_unique<DummyGetter>(tag, alias);
    };
  }
  std::string name, lower, upper, target;
  common::Value then_value, else_value;
  if (is_check_property_in_range(expr, tag, name, lower, upper, then_value,
                                 else_value)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type()
                        .data_type();

        if (type == common::DataType::INT32) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int32_t>(
                       graph, name, params.at(lower), params.at(upper)));
          return create_case_when_project(std::move(sp), then_value, else_value,
                                          alias);
        } else if (type == common::DataType::INT64) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int64_t>(
                       graph, name, params.at(lower), params.at(upper)));
          return create_case_when_project(std::move(sp), then_value, else_value,
                                          alias);
        } else if (type == common::DataType::TIMESTAMP) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<Date>(
                       graph, name, params.at(lower), params.at(upper)));
          return create_case_when_project(std::move(sp), then_value, else_value,
                                          alias);
        }
      }
      return make_project_expr(expr, alias)(graph, params, ctx);
    };
  }
  SPPredicateType ptype;
  if (is_check_property_cmp(expr, tag, name, target, then_value, else_value,
                            ptype)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type()
                        .data_type();
        if (type == common::DataType::INT32) {
          auto ptr = create_sp_pred_case_when<int32_t>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type == common::DataType::INT64) {
          auto ptr = create_sp_pred_case_when<int64_t>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type == common::DataType::TIMESTAMP) {
          auto ptr = create_sp_pred_case_when<Date>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        }
      }
      return make_project_expr(expr, alias)(graph, params, ctx);
    };
  }
  return std::nullopt;
}

std::optional<std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>>
make_project_expr(const common::Expression& expr,
                  const common::IrDataType& data_type, int alias) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    switch (data_type.data_type()) {
    case common::DataType::INT64: {
      return _make_project_expr<int64_t>(expr, alias);
    } break;
    case common::DataType::INT32: {
      return _make_project_expr<int32_t>(expr, alias);
    } break;
    case common::DataType::DOUBLE: {
      return _make_project_expr<double>(expr, alias);
    } break;
    case common::DataType::BOOLEAN: {
      return _make_project_expr<bool>(expr, alias);
    } break;
    case common::DataType::STRING: {
      return _make_project_expr<std::string_view>(expr, alias);
    } break;
    case common::DataType::TIMESTAMP: {
      return _make_project_expr<Date>(expr, alias);
    } break;
    case common::DataType::DATE32: {
      return _make_project_expr<Day>(expr, alias);
    } break;
    case common::DataType::STRING_ARRAY: {
      return [=](const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        StringArrayCollector collector(e);
        collector.builder->reserve(ctx.row_num());
        return std::make_unique<ProjectExpr<Expr, StringArrayCollector>>(
            std::move(e), collector, alias);
      };
    } break;
    // compiler bug here
    case common::DataType::NONE: {
      return make_project_expr(expr, alias);
    } break;
    default: {
      LOG(INFO) << "not support"
                << common::DataType_Name(data_type.data_type());
      return std::nullopt;
    }
    }
  }
  case common::IrDataType::kGraphType: {
    const common::GraphDataType& graph_data_type = data_type.graph_type();
    common::GraphDataType_GraphElementOpt elem_opt =
        graph_data_type.element_opt();
    int label_num = graph_data_type.graph_data_type_size();
    if (elem_opt == common::GraphDataType_GraphElementOpt::
                        GraphDataType_GraphElementOpt_VERTEX) {
      if (label_num == 1) {
        label_t v_label = static_cast<label_t>(
            graph_data_type.graph_data_type(0).label().label());
        return [=](const GraphReadInterface& graph,
                   const std::map<std::string, std::string>& params,
                   const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
          Expr e(graph, ctx, params, expr, VarType::kPathVar);
          SLVertexCollector collector(v_label);
          collector.builder.reserve(ctx.row_num());
          return std::make_unique<ProjectExpr<Expr, SLVertexCollector>>(
              std::move(e), collector, alias);
        };
      } else if (label_num > 1) {
        return [=](const GraphReadInterface& graph,
                   const std::map<std::string, std::string>& params,
                   const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
          Expr e(graph, ctx, params, expr, VarType::kPathVar);
          MLVertexCollector collector;
          collector.builder.reserve(ctx.row_num());
          return std::make_unique<ProjectExpr<Expr, MLVertexCollector>>(
              std::move(e), collector, alias);
        };
      } else {
        LOG(INFO) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      return [=](const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        EdgeCollector collector;
        return std::make_unique<ProjectExpr<Expr, EdgeCollector>>(
            std::move(e), collector, alias);
      };
    } else {
      LOG(INFO) << "unexpected type";
    }
  } break;
  case common::IrDataType::TYPE_NOT_SET: {
    return make_project_expr(expr, alias);
  } break;

  default:
    LOG(INFO) << "unexpected type"
              << common::DataType_Name(data_type.data_type());
    break;
  }
  return std::nullopt;
}

class ProjectOpr : public IReadOperator {
 public:
  ProjectOpr(const std::vector<std::function<std::unique_ptr<ProjectExprBase>(
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx)>>& exprs,
             bool is_append)
      : exprs_(exprs), is_append_(is_append) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    std::vector<std::unique_ptr<ProjectExprBase>> exprs;
    for (size_t i = 0; i < exprs_.size(); ++i) {
      exprs.push_back(exprs_[i](graph, params, ctx));
    }
    return Project::project(std::move(ctx), exprs, is_append_);
  }

 private:
  std::vector<std::function<std::unique_ptr<ProjectExprBase>(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, const Context& ctx)>>
      exprs_;
  bool is_append_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> ProjectOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  std::vector<std::function<std::unique_ptr<ProjectExprBase>(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, const Context& ctx)>>
      exprs;
  ContextMeta ret_meta;
  bool is_append = plan.plan(op_idx).opr().project().is_append();
  if (is_append) {
    ret_meta = ctx_meta;
  }
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
      const auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = m.has_alias() ? m.alias().value() : -1;
      ret_meta.set(alias);
      CHECK(m.has_expr());
      auto expr = m.expr();
      auto func = parse_special_expr(expr, alias);
      if (func.has_value()) {
        exprs.push_back(func.value());
        continue;
      }
      auto expr_builder = make_project_expr(expr, data_types.back(), alias);
      if (expr_builder.has_value()) {
        exprs.push_back(expr_builder.value());
      } else {
        exprs.push_back(make_project_expr(expr, alias));
      }
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);

      int alias = m.has_alias() ? m.alias().value() : -1;

      ret_meta.set(alias);
      CHECK(m.has_expr());
      auto expr = m.expr();
      auto func = parse_special_expr(expr, alias);
      if (func.has_value()) {
        exprs.push_back(func.value());
        continue;
      }
      if (func.has_value()) {
        exprs.push_back(func.value());
        continue;
      }

      exprs.push_back(make_project_expr(expr, alias));
    }
  }

  return std::make_pair(
      std::make_unique<ProjectOpr>(std::move(exprs), is_append), ret_meta);
  // return std::make_pair(std::make_unique<ProjectOpr>(
  //                         plan.plan(op_idx).opr().project(), data_types),
  //                   ret_meta);
}

class ProjectOrderByOpr : public IReadOperator {
 public:
  ProjectOrderByOpr(const physical::Project& project_opr,
                    const algebra::OrderBy& order_by_opr,
                    const std::vector<common::IrDataType>& data_types)
      : project_opr_(project_opr),
        order_by_opr_(order_by_opr),
        data_types_(data_types) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_project_order_by(project_opr_, order_by_opr_,
                                              graph, std::move(ctx), timer,
                                              params, data_types_);
  }

 private:
  physical::Project project_opr_;
  algebra::OrderBy order_by_opr_;
  std::vector<common::IrDataType> data_types_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta>
ProjectOrderByOprBuilder::Build(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan,
                                int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
    }
  }
  if (project_order_by_fusable_beta(plan.plan(op_idx).opr().project(),
                                    plan.plan(op_idx + 1).opr().order_by(),
                                    ctx_meta, data_types)) {
    ContextMeta ret_meta;
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      ret_meta.set(alias);
    }
    return std::make_pair(
        std::make_unique<ProjectOrderByOpr>(
            plan.plan(op_idx).opr().project(),
            plan.plan(op_idx + 1).opr().order_by(), data_types),
        ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs