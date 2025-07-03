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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SCAN_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SCAN_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
#include "flex/engines/graph_db/chunked_runtime/utils/configs.h"
#include "flex/engines/graph_db/runtime/utils/params.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/scan_state.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::ScanParams;
using gs::runtime::SPVertexPredicate;

class Scan {
 public:
  template <typename PRED_T>
  static bl::result<void> scan_vertex(const GraphReadInterface& graph,
                                      const ScanParams& params,
                                      const PRED_T& predicate,
                                      ScanOprState& state) {
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      auto vertices = graph.GetVertexSet(label);
      for (auto vid : vertices) {
        state.start_label(label);
        if (predicate(label, vid)) {
          state.collect(vid);
        }
      }
    } else if (params.tables.size() > 1) {
      for (auto label : params.tables) {
        state.start_label(label);
        auto vertices = graph.GetVertexSet(label);
        for (auto vid : vertices) {
          if (predicate(label, vid)) {
            state.collect(vid);
          }
        }
      }
    }
    state.initialize(params.alias);
    return bl::result<void>();
  }

  template <typename PRED_T>
  static bl::result<void> scan_vertex_with_limit(
      const GraphReadInterface& graph, const ScanParams& params,
      const PRED_T& predicate, ScanOprState& state) {
    int32_t cur_limit = params.limit;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      state.start_label(label);
      auto vertices = graph.GetVertexSet(label);
      for (auto vid : vertices) {
        if (cur_limit <= 0) {
          break;
        }
        if (predicate(label, vid)) {
          state.collect(vid);
          cur_limit--;
        }
      }
    } else if (params.tables.size() > 1) {
      for (auto label : params.tables) {
        if (cur_limit <= 0) {
          break;
        }
        auto vertices = graph.GetVertexSet(label);
        state.start_label(label);
        for (auto vid : vertices) {
          if (cur_limit <= 0) {
            break;
          }
          if (predicate(label, vid)) {
            state.collect(vid);
            cur_limit--;
          }
        }
      }
    }
    state.initialize(params.alias);
    return bl::result<void>();
  }

  static bl::result<void> scan_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& pred, ScanOprState& state);

  template <typename PRED_T>
  static bl::result<void> filter_gids(const GraphReadInterface& graph,
                                      const ScanParams& params,
                                      const PRED_T& predicate,
                                      const std::vector<int64_t>& gids,
                                      ScanOprState& state) {
    int32_t cur_limit = params.limit;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      state.start_label(label);
      for (auto gid : gids) {
        if (cur_limit <= 0) {
          break;
        }
        vid_t vid = GlobalId::get_vid(gid);
        if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
          state.collect(vid);
          cur_limit--;
        }
      }
    } else if (params.tables.size() > 1) {
      for (auto label : params.tables) {
        if (cur_limit <= 0) {
          break;
        }

        for (auto gid : gids) {
          if (cur_limit <= 0) {
            break;
          }
          state.start_label(label);
          vid_t vid = GlobalId::get_vid(gid);
          if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
            state.collect(vid);
            cur_limit--;
          }
        }
      }
    }
    state.initialize(params.alias);
    return bl::result<void>();
  }

  static bl::result<void> filter_gids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<int64_t>& oids,
      ScanOprState& state);

  template <typename PRED_T>
  static bl::result<void> filter_oids(const GraphReadInterface& graph,
                                      const ScanParams& params,
                                      const PRED_T& predicate,
                                      const std::vector<Any>& oids,
                                      ScanOprState& state) {
    auto limit = params.limit;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      state.start_label(label);
      for (auto oid : oids) {
        if (limit <= 0) {
          break;
        }
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid)) {
            // builder.push_back_opt(vid);
            state.collect(vid);
            --limit;
          }
        }
      }
    } else if (params.tables.size() > 1) {
      std::vector<std::pair<label_t, vid_t>> vids;

      for (auto label : params.tables) {
        if (limit <= 0) {
          break;
        }
        for (auto oid : oids) {
          if (limit <= 0) {
            break;
          }
          vid_t vid;
          if (graph.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid)) {
              vids.emplace_back(label, vid);
              --limit;
            }
          }
        }
      }
      if (vids.size() == 1) {
        state.start_label(vids[0].first);
        state.collect(vids[0].second);
      } else {
        for (auto& pair : vids) {
          state.start_label(pair.first);
          state.collect(pair.second);
        }
      }
    }
    state.initialize(params.alias);
    return bl::result<void>();
  }

  static bl::result<void> filter_oids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<Any>& oids,
      ScanOprState& state);

  static bl::result<void> find_vertex_with_oid(const GraphReadInterface& graph,
                                               label_t label, const Any& pk,
                                               int32_t alias,
                                               ScanOprState& state);

  static bl::result<void> find_vertex_with_gid(const GraphReadInterface& graph,
                                               label_t label, int64_t pk,
                                               int32_t alias,
                                               ScanOprState& state);
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif