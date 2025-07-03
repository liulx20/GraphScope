/** Copyright 2020 Alibaba Group Holding Limited.
**Licensed under the Apache License, Version 2.0(the "License");
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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_TUPLE_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_TUPLE_H_

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"
namespace gs {
namespace chunked_runtime {
class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  RTAny eval_path(size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

template <typename... Args>
class TypedTupleExpr : public ExprBase {
 public:
  TypedTupleExpr(std::array<std::unique_ptr<ExprBase>, sizeof...(Args)>&& exprs)
      : exprs_(std::move(exprs)) {
    assert(exprs.size() == sizeof...(Args));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_path_impl(std::index_sequence<Is...>, size_t idx,
                                     Arena& arena) const {
    return std::make_tuple(gs::runtime::TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_path(idx, arena))...);
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto tup = eval_path_impl(std::index_sequence_for<Args...>(), idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    gs::runtime::Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_vertex_impl(std::index_sequence<Is...>,
                                       label_t label, vid_t v,
                                       Arena& arena) const {
    return std::make_tuple(gs::runtime::TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_vertex(label, v, arena))...);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto tup =
        eval_vertex_impl(std::index_sequence_for<Args...>(), label, v, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    gs::runtime::Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_edge_impl(std::index_sequence<Is...>,
                                     const LabelTriplet& label, vid_t src,
                                     vid_t dst, const Any& data,
                                     Arena& arena) const {
    return std::make_tuple(gs::runtime::TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_edge(label, src, dst, data, arena))...);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    auto tup = eval_edge_impl(std::index_sequence_for<Args...>(), label, src,
                              dst, data, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    gs::runtime::Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  RTAnyType type() const override { return RTAnyType::kTuple; }

 private:
  std::array<std::unique_ptr<ExprBase>, sizeof...(Args)> exprs_;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_TUPLE_H_