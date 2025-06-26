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

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/const_expr.h"
namespace gs {
namespace chunked_runtime {
ConstExpr::ConstExpr(const RTAny& val) : val_(val) {
  if (val_.type() == RTAnyType::kStringValue) {
    s = val_.as_string();
    val_ = RTAny::from_string(s);
  }
}
RTAny ConstExpr::eval_path(size_t idx, Arena&) const { return val_; }
RTAny ConstExpr::eval_vertex(label_t label, vid_t v, Arena&) const {
  return val_;
}
RTAny ConstExpr::eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&,
                           Arena&) const {
  return val_;
}

RTAnyType ConstExpr::type() const { return val_.type(); }
}  // namespace chunked_runtime
}  // namespace gs