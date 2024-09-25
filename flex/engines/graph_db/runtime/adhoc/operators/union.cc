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

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {
Context eval_union(std::vector<Context>&& ctxs) {
  CHECK(ctxs.size() == 2);
  auto& ctx0 = ctxs[0];
  auto& ctx1 = ctxs[1];
  //  CHECK(ctx1.row_num() == 0);
  CHECK(ctx0.columns.size() == ctx1.columns.size());

  auto tmp = ctx0.union_ctx(ctx1);
  auto ctx = Context();
  ctx.set(0, tmp.get(3));
  ctx.set(1, tmp.get(4));
  return ctx;
}
}  // namespace runtime
}  // namespace gs