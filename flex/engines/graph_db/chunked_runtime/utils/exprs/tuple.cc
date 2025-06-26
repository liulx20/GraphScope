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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/tuple.h"
namespace gs {
namespace chunked_runtime {
TupleExpr::TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs)
    : exprs_(std::move(exprs)) {}

RTAny TupleExpr::eval_path(size_t idx, Arena& arena) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_path(idx, arena));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  arena.emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAnyType TupleExpr::type() const { return RTAnyType::kTuple; }

template <size_t N, size_t I, typename... Args>
struct TypedTupleBuilder {
  std::unique_ptr<ExprBase> build_typed_tuple(
      std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    switch (exprs[I - 1]->type()) {
    case RTAnyType::kI32Value:
      return TypedTupleBuilder<N, I - 1, int, Args...>().build_typed_tuple(
          std::move(exprs));
    case RTAnyType::kI64Value:
      return TypedTupleBuilder<N, I - 1, int64_t, Args...>().build_typed_tuple(
          std::move(exprs));
    case RTAnyType::kF64Value:
      return TypedTupleBuilder<N, I - 1, double, Args...>().build_typed_tuple(
          std::move(exprs));
    case RTAnyType::kStringValue:
      return TypedTupleBuilder<N, I - 1, std::string_view, Args...>()
          .build_typed_tuple(std::move(exprs));
    default: {
      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      for (auto& expr : exprs) {
        exprs_vec.emplace_back(std::move(expr));
      }
      return std::make_unique<TupleExpr>(std::move(exprs_vec));
    }
    }
  }
};

template <size_t N, typename... Args>
struct TypedTupleBuilder<N, 0, Args...> {
  std::unique_ptr<ExprBase> build_typed_tuple(
      std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    return std::make_unique<TypedTupleExpr<Args...>>(std::move(exprs));
  }
};

}  // namespace chunked_runtime
}  // namespace gs