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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/variable.h"

namespace gs {
namespace chunked_runtime {
RTAny VariableExpr::eval_path(size_t idx, Arena&) const {
  return var_.get(idx);
}
RTAny VariableExpr::eval_vertex(label_t label, vid_t v, Arena&) const {
  return var_.get_vertex(label, v);
}
RTAny VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, Arena&) const {
  return var_.get_edge(label, src, dst, data);
}

RTAny VariableExpr::eval_path(size_t idx, Arena&, int) const {
  return var_.get(idx, 0);
}

RTAny VariableExpr::eval_vertex(label_t label, vid_t v, Arena&, int) const {
  return var_.get_vertex(label, v, 0);
}

RTAny VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, Arena&, int) const {
  return var_.get_edge(label, src, dst, data, 0);
}

RTAnyType VariableExpr::type() const { return var_.type(); }
}  // namespace chunked_runtime
}  // namespace gs