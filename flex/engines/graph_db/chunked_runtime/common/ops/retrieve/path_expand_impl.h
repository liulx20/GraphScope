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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PATH_EXPAND_IMPL_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PATH_EXPAND_IMPL_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand_state.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/path_expand_state.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
/**
template <typename EDATA_T, typename PRED_T>
bl::result<void> single_source_shortest_path_impl(
    const GraphReadInterface& graph, size_t v_tag, label_t e_label,
    Direction dir, int lower, int upper, const PRED_T& pred) {
  std::shared_ptr<Arena> path_impls = std::make_shared<Arena>();
  label_t v_label = *input.get_vertex_labels_set(v_tag).begin();
  auto vertices = graph.GetVertexSet(v_label);
  auto dest_col_builder = SLVertexColumnBuilder::builder(v_label);
  GeneralPathColumnBuilder path_col_builder;
  std::vector<size_t> offsets;
  if (dir == Direction::kIn || dir == Direction::kOut) {
    auto view =
        (dir == Direction::kIn)
            ? graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label)
            : graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_dir(view, label, v, e_label, vertices, idx, lower, upper,
               dest_col_builder, path_col_builder, *path_impls, offsets, pred);
    });
  } else {
    CHECK(dir == Direction::kBoth);
    auto oe_view =
        graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    auto ie_view =
        graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir(oe_view, ie_view, v_label, v, e_label, vertices, idx, lower,
                    upper, dest_col_builder, path_col_builder, *path_impls,
                    offsets, pred);
    });
  }
  return std::make_tuple(dest_col_builder.finish(nullptr),
                         path_col_builder.finish(path_impls),
                         std::move(offsets));
}*/

bl::result<void> path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const DataChunk& chunk, size_t v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper, LocalEdgeExpandState& state);

template <typename EDATA_T, typename PRED_T>
void sssp_both_dir_with_order_by_length_limit(
    const GraphReadInterface::graph_view_t<EDATA_T>& view0,
    const GraphReadInterface::graph_view_t<EDATA_T>& view1, label_t v_label,
    vid_t v, const GraphReadInterface::vertex_set_t& vertices, size_t idx,
    int lower, int upper, const PRED_T& pred, int limit_upper,
    LocalSSSPState& state) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  state.init(v_label);

  GraphReadInterface::vertex_array_t<bool> vis(vertices, false);
  vis[v] = true;
  size_t cnt = 0;
  while (depth < upper && !cur.empty()) {
    if (cnt >= static_cast<size_t>(limit_upper)) {
      break;
    }
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            state.push_back(idx, u, depth);
            ++cnt;
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            state.push_back(idx, u, depth);
            ++cnt;
          }
          for (auto& e : view0.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
          for (auto& e : view1.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view0.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
        for (auto& e : view1.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename EDATA_T, typename PRED_T>
bl::result<void> single_source_shortest_path_with_order_by_length_limit_impl(
    const gs::runtime::GraphReadInterface& graph, const DataChunk& input,
    size_t start_tag, label_t e_label, gs::runtime::Direction dir, int lower,
    int upper, const PRED_T& pred, int limit_upper, LocalSSSPState& state) {
  label_t v_label = *input.get_vertex_labels_set(start_tag).begin();
  auto vertices = graph.GetVertexSet(v_label);
  {
    CHECK(dir == Direction::kBoth);
    auto oe_view =
        graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    auto ie_view =
        graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label);
    input.foreach_vertex(start_tag, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir_with_order_by_length_limit(oe_view, ie_view, v_label, v,
                                               vertices, idx, lower, upper,
                                               pred, limit_upper, state);
    });
  }

  return bl::result<void>();
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif