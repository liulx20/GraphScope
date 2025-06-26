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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_UDF_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_UDF_H_
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"
namespace gs {
namespace chunked_runtime {
class RelationshipsExpr : public ExprBase {
 public:
  RelationshipsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx, arena).as_path();
    auto rels = path.relationships();
    auto ptr = gs::runtime::ListImpl<Relation>::make_list_impl(std::move(rels));
    List rel_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(rel_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kRelation; }
  RTAnyType type() const override { return RTAnyType::kList; }

 private:
  std::unique_ptr<ExprBase> args;
};

class NodesExpr : public ExprBase {
 public:
  NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx, arena).as_path();
    auto nodes = path.nodes();
    auto ptr =
        gs::runtime::ListImpl<VertexRecord>::make_list_impl(std::move(nodes));
    List node_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(node_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kVertex; }

  RTAnyType type() const override { return RTAnyType::kList; }

 private:
  std::unique_ptr<ExprBase> args;
};

class StartNodeExpr : public ExprBase {
 public:
  StartNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.start_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }

  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class EndNodeExpr : public ExprBase {
 public:
  EndNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.end_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class ToFloatExpr : public ExprBase {
 public:
  static double to_double(const RTAny& val) {
    if (val.type() == RTAnyType::kI64Value) {
      return static_cast<double>(val.as_int64());
    } else if (val.type() == RTAnyType::kI32Value) {
      return static_cast<double>(val.as_int32());
    } else if (val.type() == RTAnyType::kF64Value) {
      return val.as_double();
    } else {
      LOG(FATAL) << "invalid type";
    }
  }

  ToFloatExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto val = args->eval_path(idx, arena, 0);
    if (val.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto val = args->eval_vertex(label, v, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    auto val = args->eval_edge(label, src, dst, data, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAnyType type() const override { return RTAnyType::kF64Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class StrConcatExpr : public ExprBase {
 public:
  StrConcatExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::string ret = std::string(lhs->eval_path(idx, arena).as_string()) +
                      ";" + std::string(rhs->eval_path(idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (lhs->eval_path(idx, arena, 0).is_null() ||
        rhs->eval_path(idx, arena, 0).is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    std::string ret =
        std::string(lhs->eval_vertex(label, v, arena).as_string()) + ";" +
        std::string(rhs->eval_vertex(label, v, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    std::string ret =
        std::string(lhs->eval_edge(label, src, dst, data, arena).as_string()) +
        ";" +
        std::string(rhs->eval_edge(label, src, dst, data, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAnyType type() const override { return RTAnyType::kStringValue; }
  bool is_optional() const override {
    return lhs->is_optional() || rhs->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs;
  std::unique_ptr<ExprBase> rhs;
};

class StrListSizeExpr : public ExprBase {
 public:
  StrListSizeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}

  RTAny eval_path(size_t idx, Arena& arena) const override {
    CHECK(args->type() == RTAnyType::kStringValue);
    auto str_list = args->eval_path(idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto list = args->eval_path(idx, arena, 0);
    if (list.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto str_list = args->eval_vertex(label, v, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    auto str_list = args->eval_edge(label, src, dst, data, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  int32_t _size(const std::string_view& sv) const {
    if (sv.empty()) {
      return 0;
    }
    int64_t ret = 1;
    for (auto c : sv) {
      if (c == ';') {
        ++ret;
      }
    }
    return ret;
  }
  std::unique_ptr<ExprBase> args;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif