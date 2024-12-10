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

#include "flex/engines/graph_db/runtime/adhoc/operators/update/operators.h"

namespace gs {

namespace runtime {

// for insert transaction, lazy evaluation as we don't have the type
// information
WriteContext eval_project(const physical::Project& opr,
                          const GraphInsertInterface& graph, WriteContext&& ctx,
                          const std::map<std::string, std::string>& params) {
  int mappings_size = opr.mappings_size();
  WriteContext ret;
  std::unordered_map<int, std::pair<WriteContext::WriteParamsColumn,
                                    WriteContext::WriteParamsColumn>>
      pairs_cache;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = opr.mappings(i);
    CHECK(m.has_alias());
    CHECK(m.has_expr());
    CHECK(m.expr().operators_size() == 1);

    if (m.expr().operators(0).item_case() == common::ExprOpr::kParam) {
      auto param = m.expr().operators(0).param();
      CHECK(params.find(param.name()) != params.end());
      const auto& value = params.at(param.name());
      WriteContext::WriteParams p(value);
      ret.set(m.alias().value(), WriteContext::WriteParamsColumn({p}));
    } else if (m.expr().operators(0).item_case() == common::ExprOpr::kVar) {
      auto var = m.expr().operators(0).var();
      CHECK(var.has_tag());
      CHECK(!var.has_property());
      int tag = var.tag().id();
      ret.set(m.alias().value(), std::move(ctx.get(tag)));
    } else if (m.expr().operators(0).has_udf_func()) {
      auto udf_func = m.expr().operators(0).udf_func();
      if (udf_func.name() == "gs.function.first") {
        CHECK(udf_func.parameters_size() == 1 &&
              udf_func.parameters(0).operators_size() == 1);
        auto param = udf_func.parameters(0).operators(0);

        CHECK(param.item_case() == common::ExprOpr::kVar);
        auto var = param.var();
        CHECK(var.has_tag());
        CHECK(!var.has_property());
        int tag = var.tag().id();
        if (pairs_cache.count(tag)) {
          auto& pair = pairs_cache[tag];
          ret.set(m.alias().value(), std::move(pair.first));
        } else {
          auto col = ctx.get(tag);
          auto [first, second] = col.pairs();
          pairs_cache[tag] = {first, second};
          ret.set(m.alias().value(), std::move(first));
        }
      } else if (udf_func.name() == "gs.function.second") {
        CHECK(udf_func.parameters_size() == 1 &&
              udf_func.parameters(0).operators_size() == 1);
        auto param = udf_func.parameters(0).operators(0);

        CHECK(param.item_case() == common::ExprOpr::kVar);
        auto var = param.var();
        CHECK(var.has_tag());
        CHECK(!var.has_property());
        int tag = var.tag().id();
        if (pairs_cache.count(tag)) {
          auto& pair = pairs_cache[tag];
          ret.set(m.alias().value(), std::move(pair.second));
        } else {
          auto col = ctx.get(tag);
          auto [first, second] = col.pairs();
          pairs_cache[tag] = {first, second};
          ret.set(m.alias().value(), std::move(second));
        }
      } else {
        LOG(FATAL) << "not support for " << m.expr().DebugString();
      }
    } else {
      LOG(FATAL) << "not support for " << m.expr().DebugString();
    }
  }

  return ret;
}

}  // namespace runtime

}  // namespace gs