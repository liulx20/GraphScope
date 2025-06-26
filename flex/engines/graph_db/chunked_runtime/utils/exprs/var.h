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

#ifndef CHUNKED_RUNTIME_UTILS_VAR_H_
#define CHUNKED_RUNTIME_UTILS_VAR_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/accessor.h"
#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/types.h"

#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace chunked_runtime {

using gs::runtime::GraphReadInterface;
using gs::runtime::GraphUpdateInterface;

using gs::runtime::LabelTriplet;
using gs::runtime::RTAny;
using gs::runtime::RTAnyType;
enum class VarType {
  kVertexVar,
  kEdgeVar,
  kPathVar,
};

class Var {
 public:
  template <typename GraphInterface>
  Var(const GraphInterface& graph, const DataChunk& ctx,
      const common::Variable& pb, VarType var_type);
  ~Var();

  RTAny get(size_t path_idx) const;
  RTAny get_vertex(label_t label, vid_t v) const;
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data) const;

  RTAny get(size_t path_idx, int) const;
  RTAny get_vertex(label_t label, vid_t v, int) const;
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data, int) const;
  RTAnyType type() const;
  bool is_optional() const { return getter_->is_optional(); }

 private:
  std::shared_ptr<IAccessor> getter_;
  RTAnyType type_;
};

}  // namespace chunked_runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_VAR_H_