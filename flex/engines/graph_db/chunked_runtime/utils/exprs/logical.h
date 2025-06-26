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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_LOGICAL_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_LOGICAL_H_
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"
namespace gs {
namespace chunked_runtime {
class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena&) const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
  RTAnyType type() const override;

  bool is_optional() const override { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
  common::Logical logic_;
};
class LogicalExpr : public ExprBase {
 public:
  LogicalExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
              common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena&) const override;

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= lhs_->eval_path(idx, arena, 0).as_bool();
      }
      if (!rhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= rhs_->eval_path(idx, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }

    if (lhs_->eval_path(idx, arena, 0).is_null() ||
        rhs_->eval_path(idx, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena, int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_vertex(label, v, arena, 0).is_null()) {
        flag |= lhs_->eval_vertex(label, v, arena, 0).as_bool();
      }
      if (!rhs_->eval_vertex(label, v, arena, 0).is_null()) {
        flag |= rhs_->eval_vertex(label, v, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }
    if (lhs_->eval_vertex(label, v, arena, 0).is_null() ||
        rhs_->eval_vertex(label, v, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, arena);
  }

  RTAnyType type() const override;

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<bool(RTAny, RTAny)> op_;
  common::Logical logic_;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_LOGICAL_H_