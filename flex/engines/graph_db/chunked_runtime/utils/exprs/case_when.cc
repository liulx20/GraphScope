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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/case_when.h"
namespace gs {
namespace chunked_runtime {
CaseWhenExpr::CaseWhenExpr(
    std::vector<std::pair<std::unique_ptr<ExprBase>,
                          std::unique_ptr<ExprBase>>>&& when_then_exprs,
    std::unique_ptr<ExprBase>&& else_expr)
    : when_then_exprs_(std::move(when_then_exprs)),
      else_expr_(std::move(else_expr)) {}

RTAny CaseWhenExpr::eval_path(size_t idx, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_path(idx, arena).as_bool()) {
      return pair.second->eval_path(idx, arena);
    }
  }
  return else_expr_->eval_path(idx, arena);
}

RTAny CaseWhenExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_vertex(label, v, arena).as_bool()) {
      return pair.second->eval_vertex(label, v, arena);
    }
  }
  return else_expr_->eval_vertex(label, v, arena);
}

RTAny CaseWhenExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, Arena& arena) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_edge(label, src, dst, data, arena).as_bool()) {
      return pair.second->eval_edge(label, src, dst, data, arena);
    }
  }
  return else_expr_->eval_edge(label, src, dst, data, arena);
}

RTAnyType CaseWhenExpr::type() const {
  RTAnyType type(RTAnyType::kNull);
  if (when_then_exprs_.size() > 0) {
    if (when_then_exprs_[0].second->type() != RTAnyType::kNull) {
      type = when_then_exprs_[0].second->type();
    }
  }
  if (else_expr_->type() != RTAnyType::kNull) {
    type = else_expr_->type();
  }
  return type;
}
}  // namespace chunked_runtime
}  // namespace gs