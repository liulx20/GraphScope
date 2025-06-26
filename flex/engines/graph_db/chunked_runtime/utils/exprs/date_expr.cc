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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/date_expr.h"
namespace gs {
namespace chunked_runtime {
DateMinusExpr::DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                             std::unique_ptr<ExprBase>&& rhs)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

RTAny DateMinusExpr::eval_path(size_t idx, Arena& arena) const {
  auto lhs = lhs_->eval_path(idx, arena).as_timestamp();
  auto rhs = rhs_->eval_path(idx, arena).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_vertex(label_t label, vid_t v, Arena& arena) const {
  auto lhs = lhs_->eval_vertex(label, v, arena).as_timestamp();
  auto rhs = rhs_->eval_vertex(label, v, arena).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                               const Any& data, Arena& arena) const {
  auto lhs = lhs_->eval_edge(label, src, dst, data, arena).as_timestamp();
  auto rhs = rhs_->eval_edge(label, src, dst, data, arena).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAnyType DateMinusExpr::type() const { return RTAnyType::kI64Value; }

static int32_t extract_year(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_year + 1900;
}

static int32_t extract_month(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mon + 1;
}

static int32_t extract_day(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mday;
}

int32_t extract_time_from_milli_second(int64_t ms, common::Extract extract) {
  if (extract.interval() == common::Extract::YEAR) {
    return extract_year(ms);
  } else if (extract.interval() == common::Extract::MONTH) {
    return extract_month(ms);
  } else if (extract.interval() == common::Extract::DAY) {
    return extract_day(ms);
  } else {
    LOG(FATAL) << "not support";
  }
  return 0;
}
}  // namespace chunked_runtime
}  // namespace gs