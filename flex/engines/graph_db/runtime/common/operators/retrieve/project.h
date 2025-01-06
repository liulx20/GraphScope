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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_

#include <tuple>
#include <type_traits>

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

struct ProjectExprBase {
  virtual ~ProjectExprBase() = default;
  virtual Context evaluate(const Context& ctx, Context&& ret) = 0;
};

struct DummyGetter : public ProjectExprBase {
  DummyGetter(int from, int to) : from_(from), to_(to) {}
  Context evaluate(const Context& ctx, Context&& ret) override {
    ret.set(to_, ctx.get(from_));
    return ret;
  }

  int from_;
  int to_;
};

template <typename EXPR, typename COLLECTOR_T>
struct ProjectExpr : public ProjectExprBase {
  EXPR expr_;
  COLLECTOR_T collector_;
  int alias_;

  ProjectExpr(EXPR&& expr, const COLLECTOR_T& collector, int alias)
      : expr_(std::move(expr)), collector_(collector), alias_(alias) {}

  Context evaluate(const Context& ctx, Context&& ret) override {
    size_t row_num = ctx.row_num();
    for (size_t i = 0; i < row_num; ++i) {
      collector_.collect(expr_, i);
    }
    ret.set(alias_, collector_.get(expr_));
    return ret;
  }
};

class Project {
 public:
  static Context project(
      Context&& ctx, const std::vector<std::unique_ptr<ProjectExprBase>>& exprs,
      bool is_append = false) {
    Context ret;
    if (is_append) {
      ret = ctx;
    }
    for (size_t i = 0; i < exprs.size(); ++i) {
      ret = exprs[i]->evaluate(ctx, std::move(ret));
    }
    return ret;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_