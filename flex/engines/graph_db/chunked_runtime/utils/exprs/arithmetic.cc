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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/arithmetic.h"

namespace gs {
namespace chunked_runtime {
ArithExpr::ArithExpr(std::unique_ptr<ExprBase>&& lhs,
                     std::unique_ptr<ExprBase>&& rhs, common::Arithmetic arith)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), arith_(arith) {
  switch (arith_) {
  case common::Arithmetic::ADD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs + rhs; };
    break;
  }
  case common::Arithmetic::SUB: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs - rhs; };
    break;
  }
  case common::Arithmetic::DIV: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs / rhs; };
    break;
  }
  case common::Arithmetic::MOD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs % rhs; };
    break;
  }

  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(arith);
    break;
  }
  }
}

RTAny ArithExpr::eval_path(size_t idx, Arena& arena) const {
  return op_(lhs_->eval_path(idx, arena), rhs_->eval_path(idx, arena));
}

RTAny ArithExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  return op_(lhs_->eval_vertex(label, v, arena),
             rhs_->eval_vertex(label, v, arena));
}

RTAny ArithExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, Arena& arena) const {
  return op_(lhs_->eval_edge(label, src, dst, data, arena),
             rhs_->eval_edge(label, src, dst, data, arena));
}

RTAnyType ArithExpr::type() const {
  if (lhs_->type() == RTAnyType::kF64Value ||
      rhs_->type() == RTAnyType::kF64Value) {
    return RTAnyType::kF64Value;
  }
  if (lhs_->type() == RTAnyType::kI64Value ||
      rhs_->type() == RTAnyType::kI64Value) {
    return RTAnyType::kI64Value;
  }
  return lhs_->type();
}

}  // namespace chunked_runtime
}  // namespace gs