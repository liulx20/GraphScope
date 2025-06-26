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
#ifndef CHUNKED_RUNTIME_UTILS_PREDICATES_H_
#define CHUNKED_RUNTIME_UTILS_PREDICATES_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/utils/expr.h"
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/var.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace chunked_runtime {

struct GeneralVertexPredicate {
  GeneralVertexPredicate(const GraphReadInterface& graph, const DataChunk& ctx,
                         const std::map<std::string, std::string>& params,
                         const common::Expression& expr)
      : expr_(graph, ctx, params, expr, VarType::kVertexVar) {}

  inline bool operator()(label_t label, vid_t v, Arena& arena) const {
    auto val = expr_.eval_vertex(label, v, arena);
    return val.as_bool();
  }

  inline bool operator()(label_t label, vid_t v, Arena& arena, int) const {
    auto val = expr_.eval_vertex(label, v, arena, 0);
    return val.as_bool();
  }

  Expr expr_;
};

struct ExactVertexPredicate {
  ExactVertexPredicate(label_t label, vid_t vid) : label_(label), vid_(vid) {}

  inline bool operator()(label_t label, vid_t vid) const {
    return (label == label_) && (vid == vid_);
  }

  label_t label_;
  vid_t vid_;
};

struct GeneralEdgePredicate {
  GeneralEdgePredicate(const GraphReadInterface& graph, const DataChunk& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(graph, ctx, params, expr, VarType::kEdgeVar) {}

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, Arena& arena) const {
    auto val = expr_.eval_edge(label, src, dst, edata, arena);
    return val.as_bool();
  }

  Expr expr_;
};

struct DummyVertexPredicate {
  bool operator()(label_t label, vid_t v) const { return true; }

  // for optional vertex
  inline bool operator()(label_t label, vid_t v, int) const { return true; }
};

struct DummyEdgePredicate {
  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir) const {
    return true;
  }
};

}  // namespace chunked_runtime

}  // namespace gs

#endif  // CHUNKED_RUNTIME_UTILS_PREDICATES_H_