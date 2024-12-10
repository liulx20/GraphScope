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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/limit.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/retrieve/operators.h"
#include "flex/proto_generated_gie/algebra.pb.h"

namespace gs {
namespace runtime {

Context eval_limit(const algebra::Limit& opr, Context&& ctx) {
  int lower = 0;
  int upper = ctx.row_num();

  if (opr.has_range()) {
    lower = std::max(lower, static_cast<int>(opr.range().lower()));
    upper = std::min(upper, static_cast<int>(opr.range().upper()));
  }
  CHECK(lower >= 0 && upper >= 0 && lower <= upper);

  return Limit::limit(std::move(ctx), lower, upper);
}
}  // namespace runtime
}  // namespace gs
