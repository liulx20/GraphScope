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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_DATE_EXPR_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_DATE_EXPR_H_
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"

namespace gs {
namespace chunked_runtime {

int32_t extract_time_from_milli_second(int64_t ms, common::Extract extract);

template <typename T>
class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr, const common::Extract& extract)
      : expr_(std::move(expr)), extract_(extract) {}
  int32_t eval_impl(const RTAny& val) const {
    if constexpr (std::is_same_v<T, int64_t>) {
      return extract_time_from_milli_second(val.as_int64(), extract_);
    } else if constexpr (std::is_same_v<T, Date>) {
      return extract_time_from_milli_second(val.as_timestamp().milli_second,
                                            extract_);

    } else if constexpr (std::is_same_v<T, Day>) {
      if (extract_.interval() == common::Extract::DAY) {
        return val.as_date32().day();
      } else if (extract_.interval() == common::Extract::MONTH) {
        return val.as_date32().month();
      } else if (extract_.interval() == common::Extract::YEAR) {
        return val.as_date32().year();
      }
    }
    LOG(FATAL) << "not support" << extract_.DebugString();
    return 0;
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    return RTAny::from_int32(eval_impl(expr_->eval_path(idx, arena)));
  }
  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    return RTAny::from_int32(eval_impl(expr_->eval_vertex(label, v, arena)));
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    return RTAny::from_int32(
        eval_impl(expr_->eval_edge(label, src, dst, data, arena)));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }

 private:
  std::unique_ptr<ExprBase> expr_;
  const common::Extract extract_;
};
class DateMinusExpr : public ExprBase {
 public:
  DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
};
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_DATE_EXPR_H_
