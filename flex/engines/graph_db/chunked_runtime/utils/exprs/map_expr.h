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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_MAP_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_MAP_H_
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"
namespace gs {
namespace chunked_runtime {

class MapExpr : public ExprBase {
 public:
  MapExpr(std::vector<RTAny>&& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : keys(std::move(keys)), value_exprs(std::move(values)) {
    assert(keys.size() == values.size());
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena));
    }
    auto map_impl = MapImpl::make_map_impl(keys, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena, 0));
    }
    auto map_impl = MapImpl::make_map_impl(keys, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAnyType type() const override { return RTAnyType::kMap; }

  bool is_optional() const override {
    for (auto& expr : value_exprs) {
      if (expr->is_optional()) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<RTAny> keys;
  std::vector<std::unique_ptr<ExprBase>> value_exprs;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_MAP_H_