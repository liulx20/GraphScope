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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/logical.h"

namespace gs {
namespace chunked_runtime {
LogicalExpr::LogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                         std::unique_ptr<ExprBase>&& rhs, common::Logical logic)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), logic_(logic) {
  switch (logic) {
  case common::Logical::LT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs < rhs; };
    break;
  }
  case common::Logical::GT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return rhs < lhs; };
    break;
  }
  case common::Logical::GE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs < rhs); };
    break;
  }
  case common::Logical::LE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(rhs < lhs); };
    break;
  }
  case common::Logical::EQ: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs == rhs; };
    break;
  }
  case common::Logical::NE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs == rhs); };
    break;
  }
  case common::Logical::AND: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() && rhs.as_bool();
    };
    break;
  }
  case common::Logical::OR: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() || rhs.as_bool();
    };
    break;
  }
  case common::Logical::REGEX: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      auto lhs_str = std::string(lhs.as_string());
      auto rhs_str = std::string(rhs.as_string());
      return std::regex_match(lhs_str, std::regex(rhs_str));
    };
    break;
  }
  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(logic);
    break;
  }
  }
}

RTAny LogicalExpr::eval_path(size_t idx, Arena& arena) const {
  return RTAny::from_bool(
      op_(lhs_->eval_path(idx, arena), rhs_->eval_path(idx, arena)));
}

RTAny LogicalExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  return RTAny::from_bool(op_(lhs_->eval_vertex(label, v, arena),
                              rhs_->eval_vertex(label, v, arena)));
}

RTAny LogicalExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const Any& data, Arena& arena) const {
  return RTAny::from_bool(op_(lhs_->eval_edge(label, src, dst, data, arena),
                              rhs_->eval_edge(label, src, dst, data, arena)));
}

RTAnyType LogicalExpr::type() const { return RTAnyType::kBoolValue; }

UnaryLogicalExpr::UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr,
                                   common::Logical logic)
    : expr_(std::move(expr)), logic_(logic) {}

RTAny UnaryLogicalExpr::eval_path(size_t idx, Arena& arena) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_path(idx, arena).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_path(idx, arena, 0).type() ==
                            RTAnyType::kNull);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_path(size_t idx, Arena& arena, int) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_path(idx, arena, 0).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_path(idx, arena, 0).type() ==
                            RTAnyType::kNull);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_vertex(label_t label, vid_t v,
                                    Arena& arena) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_vertex(label, v, arena).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_vertex(label, v, arena, 0).is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const Any& data,
                                  Arena& arena) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(
        !expr_->eval_edge(label, src, dst, data, arena).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(
        expr_->eval_edge(label, src, dst, data, arena, 0).is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAnyType UnaryLogicalExpr::type() const { return RTAnyType::kBoolValue; }
}  // namespace chunked_runtime
}  // namespace gs