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

#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/dedup.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
bl::result<ReadOpBuildResultT> DedupOprBuilder::Build(
    std::unique_ptr<IReadOpr>&, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  const auto& dedup = plan.plan(op_idx).opr().dedup();
  LOG(INFO) << dedup.DebugString();
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs