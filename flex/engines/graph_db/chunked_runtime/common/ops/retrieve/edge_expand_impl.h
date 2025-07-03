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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_IMPL_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_IMPL_H_

#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand_state.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
inline bool check_exist_special_edge(const GraphReadInterface& graph,
                                     const std::vector<LabelTriplet>& labels,
                                     Direction dir) {
  for (auto& triplet : labels) {
    if (graph.schema().exist(triplet.src_label, triplet.dst_label,
                             triplet.edge_label)) {
      if ((dir == Direction::kOut) || (dir == Direction::kBoth)) {
        if (graph.schema().get_outgoing_edge_strategy(
                triplet.src_label, triplet.dst_label, triplet.edge_label) !=
            EdgeStrategy::kMultiple) {
          return true;
        }
      }
      if ((dir == Direction::kIn) || (dir == Direction::kBoth)) {
        if (graph.schema().get_incoming_edge_strategy(
                triplet.src_label, triplet.dst_label, triplet.edge_label) !=
            EdgeStrategy::kMultiple) {
          return true;
        }
      }
    }
  }
  return false;
}
template <typename EDATA_T, typename PRED_T>
bool expand_vertex_on_graph_view(
    const GraphReadInterface::graph_view_t<EDATA_T>& view,
    const DataChunk& chunk, int v_tag, label_t nbr_label, label_t e_label,
    Direction dir, const PRED_T& pred, LocalEdgeExpandState& state) {
  label_t input_label = *chunk.get_vertex_labels_set(v_tag).begin();

  auto collector = state.getEdgeCollector<SLVertexColumn, label_t>(nbr_label);
  chunk.foreach_vertex(v_tag, [&](size_t index, label_t l, vid_t v) {
    auto es = view.get_edges(v);
    for (auto& e : es) {
      if (pred(input_label, v, nbr_label, e.get_neighbor(), e_label, dir,
               e.get_data())) {
        collector.push_back_opt(index, e.get_neighbor());
      }
    }
  });
  return true;
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_se(const GraphReadInterface& graph,
                                const DataChunk& input, int v_tag,
                                label_t nbr_label, label_t edge_label,
                                Direction dir, const PRED_T& pred,
                                LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  GraphReadInterface::graph_view_t<EDATA_T> view =
      (dir == Direction::kIn) ? graph.GetIncomingGraphView<EDATA_T>(
                                    input_label, nbr_label, edge_label)
                              : graph.GetOutgoingGraphView<EDATA_T>(
                                    input_label, nbr_label, edge_label);
  return expand_vertex_on_graph_view(view, input, v_tag, nbr_label, edge_label,
                                     dir, pred, state);
}

template <typename EDATA_T, typename PRED_T>
bool expand_vertex_on_graph_view_optional(
    const GraphReadInterface::graph_view_t<EDATA_T>& view,
    const DataChunk& chunk, int v_tag, label_t nbr_label, label_t e_label,
    Direction dir, const PRED_T& pred, LocalEdgeExpandState& state) {
  label_t input_label = *chunk.get_vertex_labels_set(v_tag).begin();
  auto collector = state.getEdgeCollector<SLVertexColumn, label_t>(input_label);
  if (chunk.is_optional(v_tag)) {
    chunk.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
      if (v == std::numeric_limits<vid_t>::max()) {
        collector.push_back_null(idx);
        return;
      }
      bool found = false;
      auto es = view.get_edges(v);
      for (auto& e : es) {
        if (pred(input_label, v, nbr_label, e.get_neighbor(), e_label, dir,
                 e.get_data())) {
          collector.push_back_opt(idx, e.get_neighbor());

          found = true;
        }
      }
      if (!found) {
        collector.push_back_null(idx);
      }
    });
  } else {
    chunk.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
      bool found = false;
      auto es = view.get_edges(v);
      for (auto& e : es) {
        if (pred(input_label, v, nbr_label, e.get_neighbor(), e_label, dir,
                 e.get_data())) {
          collector.push_back_opt(idx, e.get_neighbor());
          found = true;
        }
      }
      if (!found) {
        collector.push_back_null(idx);
      }
    });
  }

  return true;
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_se_optional(const GraphReadInterface& graph,
                                         const DataChunk& input, int v_tag,
                                         label_t nbr_label, label_t edge_label,
                                         Direction dir, const PRED_T& pred,
                                         LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  GraphReadInterface::graph_view_t<EDATA_T> view =
      (dir == Direction::kIn) ? graph.GetIncomingGraphView<EDATA_T>(
                                    input_label, nbr_label, edge_label)
                              : graph.GetOutgoingGraphView<EDATA_T>(
                                    input_label, nbr_label, edge_label);
  return expand_vertex_on_graph_view_optional(view, input, v_tag, nbr_label,
                                              edge_label, dir, pred, state);
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_me_sp(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  std::vector<GraphReadInterface::graph_view_t<EDATA_T>> views;
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  std::vector<label_t> nbr_labels;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels.push_back(nbr_label);
    if (dir == Direction::kOut) {
      views.emplace_back(graph.GetOutgoingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(graph.GetIncomingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label));
    }
  }

  bool single_nbr_label = true;
  for (size_t k = 1; k < nbr_labels.size(); ++k) {
    if (nbr_labels[k] != nbr_labels[0]) {
      single_nbr_label = false;
      break;
    }
  }
  if (single_nbr_label) {
    auto builder =
        state.getEdgeCollector<SLVertexColumn, label_t>(nbr_labels[0]);
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
      size_t csr_idx = 0;
      for (auto& csr : views) {
        label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
        label_t edge_label = std::get<1>(label_dirs[csr_idx]);
        Direction dir = std::get<2>(label_dirs[csr_idx]);
        auto es = csr.get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, e.get_neighbor());
          }
        }
        ++csr_idx;
      }
    });

  } else {
    auto builder = state.getEdgeCollector<MLVertexColumn>();
    size_t csr_idx = 0;
    for (auto& csr : views) {
      label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
      label_t edge_label = std::get<1>(label_dirs[csr_idx]);
      Direction dir = std::get<2>(label_dirs[csr_idx]);

      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
        auto es = csr.get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, nbr_label, e.get_neighbor());
          }
        }
      });
      ++csr_idx;
    }
  }

  return true;
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_me_sp_optional(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  std::vector<GraphReadInterface::graph_view_t<EDATA_T>> views;
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  std::vector<label_t> nbr_labels;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels.push_back(nbr_label);
    if (dir == Direction::kOut) {
      views.emplace_back(graph.GetOutgoingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(graph.GetIncomingGraphView<EDATA_T>(
          input_label, nbr_label, edge_label));
    }
  }

  bool single_nbr_label = true;
  for (size_t k = 1; k < nbr_labels.size(); ++k) {
    if (nbr_labels[k] != nbr_labels[0]) {
      single_nbr_label = false;
      break;
    }
  }
  if (single_nbr_label) {
    auto collector =
        state.getEdgeCollector<SLVertexColumn, label_t>(nbr_labels[0]);
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
      if (v == std::numeric_limits<vid_t>::max()) {
        collector.push_back_null(idx);
        return;
      }
      bool found = false;
      size_t csr_idx = 0;
      for (auto& csr : views) {
        label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
        label_t edge_label = std::get<1>(label_dirs[csr_idx]);
        Direction dir = std::get<2>(label_dirs[csr_idx]);
        auto es = csr.get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            collector.push_back_opt(idx, e.get_neighbor());
            found = true;
          }
        }
        ++csr_idx;
      }
      if (!found) {
        collector.push_back_null(idx);
      }
      ++idx;
    });
  } else {
    auto collector = state.getEdgeCollector<MLVertexColumn>();
    size_t csr_idx = 0;
    for (auto& csr : views) {
      label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
      label_t edge_label = std::get<1>(label_dirs[csr_idx]);
      Direction dir = std::get<2>(label_dirs[csr_idx]);
      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
        if (v == std::numeric_limits<vid_t>::max()) {
          collector.push_back_null(idx);
          return;
        }
        bool found = false;
        auto es = csr.get_edges(v);
        for (auto& e : es) {
          if (pred(input_label, v, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            collector.push_back_opt(idx, nbr_label, e.get_neighbor());
            found = true;
          }
        }
        // fix me
        if (!found) {
          collector.push_back_null(idx);
        }
      });

      ++csr_idx;
    }
  }
  return true;
}
template <typename PRED_T>
inline bool expand_vertex_np_me_mp(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& labels,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  auto builder = state.getEdgeCollector<MLVertexColumn>();
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();

  input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t v) {
    for (auto& t : labels) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it = (dir == Direction::kOut)
                    ? (graph.GetOutEdgeIterator(input_label, v, nbr_label,
                                                edge_label))
                    : (graph.GetInEdgeIterator(input_label, v, nbr_label,
                                               edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                 it.GetData())) {
          builder.push_back_opt(idx, nbr_label, nbr);
        }
        it.Next();
      }
    }
  });
  return true;
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_se(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  int label_num = label_dirs.size();
  std::vector<GraphReadInterface::graph_view_t<EDATA_T>> views(label_num);
  std::vector<label_t> nbr_labels(label_num,
                                  std::numeric_limits<label_t>::max());
  std::vector<label_t> edge_labels(label_num,
                                   std::numeric_limits<label_t>::max());
  std::vector<Direction> dirs(label_num);
  std::set<label_t> nbr_labels_set;
  bool all_exist = true;
  for (auto i : input.get_vertex_labels_set(v_tag)) {
    if (label_dirs[i].empty()) {
      all_exist = false;
      continue;
    }
    auto& t = label_dirs[i][0];
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels[i] = nbr_label;
    edge_labels[i] = edge_label;
    dirs[i] = dir;
    nbr_labels_set.insert(nbr_label);
    if (dir == Direction::kOut) {
      views[i] = graph.GetOutgoingGraphView<EDATA_T>(static_cast<label_t>(i),
                                                     nbr_label, edge_label);
    } else {
      CHECK(dir == Direction::kIn);
      views[i] = graph.GetIncomingGraphView<EDATA_T>(static_cast<label_t>(i),
                                                     nbr_label, edge_label);
    }
  }

  if (nbr_labels_set.size() == 1) {
    auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(
        *nbr_labels_set.begin());
    if (all_exist) {
      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
        auto es = views[l].get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_labels[l], e.get_neighbor(), edge_labels[l],
                   dirs[l], e.get_data())) {
            builder.push_back_opt(idx, e.get_neighbor());
          }
        }
      });
    } else {
      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
        if (!views[l].is_null()) {
          auto es = views[l].get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.get_neighbor(), edge_labels[l],
                     dirs[l], e.get_data())) {
              builder.push_back_opt(idx, e.get_neighbor());
            }
          }
        }
      });
    }

  } else {
    auto builder = state.getEdgeCollector<MLVertexColumn>();
    if (all_exist) {
      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
        auto es = views[l].get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_labels[l], e.get_neighbor(), edge_labels[l],
                   dirs[l], e.get_data())) {
            builder.push_back_opt(idx, nbr_labels[l], e.get_neighbor());
          }
        }
      });
    } else {
      input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
        if (!views[l].is_null()) {
          auto es = views[l].get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.get_neighbor(), edge_labels[l],
                     dirs[l], e.get_data())) {
              builder.push_back_opt(idx, nbr_labels[l], e.get_neighbor());
            }
          }
        }
      });
    }
  }
  return true;
}
/**
template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_se(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  int label_num = label_dirs.size();
  std::vector<GraphReadInterface::graph_view_t<EDATA_T>> views(label_num);
  std::vector<label_t> nbr_labels(label_num,
                                  std::numeric_limits<label_t>::max());
  std::vector<label_t> edge_labels(label_num,
                                   std::numeric_limits<label_t>::max());
  std::vector<Direction> dirs(label_num);
  std::set<label_t> nbr_labels_set;
  for (auto i : input.get_labels_set()) {
    if (label_dirs[i].empty()) {
      continue;
    }
    auto& t = label_dirs[i][0];
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    nbr_labels[i] = nbr_label;
    edge_labels[i] = edge_label;
    dirs[i] = dir;
    nbr_labels_set.insert(nbr_label);
    if (dir == Direction::kOut) {
      views[i] = graph.GetOutgoingGraphView<EDATA_T>(static_cast<label_t>(i),
                                                     nbr_label, edge_label);
    } else {
      CHECK(dir == Direction::kIn);
      views[i] = graph.GetIncomingGraphView<EDATA_T>(static_cast<label_t>(i),
                                                     nbr_label, edge_label);
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    auto builder = SLVertexColumnBuilder::builder(*nbr_labels_set.begin());
    size_t input_seg_num = input.seg_num();
    size_t idx = 0;
    for (size_t k = 0; k < input_seg_num; ++k) {
      label_t l = input.seg_label(k);
      auto& view = views[l];
      if (!view.is_null()) {
        for (auto vid : input.seg_vertices(k)) {
          auto es = view.get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_labels[l], e.get_neighbor(), edge_labels[l],
                     dirs[l], e.get_data())) {
              builder.push_back_opt(e.get_neighbor());
              offsets.push_back(idx);
            }
          }
          ++idx;
        }
      } else {
        idx += input.seg_vertices(k).size();
      }
    }
    col = builder.finish(nullptr);
  } else {
    size_t idx = 0;
    auto builder = MSVertexColumnBuilder::builder();
    size_t input_seg_num = input.seg_num();
    for (size_t k = 0; k < input_seg_num; ++k) {
      label_t l = input.seg_label(k);
      auto& view = views[l];
      if (!view.is_null()) {
        label_t nbr_label = nbr_labels[l];
        builder.start_label(nbr_label);
        for (auto vid : input.seg_vertices(k)) {
          auto es = view.get_edges(vid);
          for (auto& e : es) {
            if (pred(l, vid, nbr_label, e.get_neighbor(), edge_labels[l],
                     dirs[l], e.get_data())) {
              builder.push_back_opt(e.get_neighbor());
              offsets.push_back(idx);
            }
          }
          ++idx;
        }
      } else {
        idx += input.seg_vertices(k).size();
      }
    }
    col = builder.finish(nullptr);
  }
  return std::make_pair(col, std::move(offsets));
  return true;
}
*/
template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_me_sp(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphReadInterface::graph_view_t<EDATA_T>>> views(
      label_num);
  std::set<label_t> nbr_labels_set;
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
      label_dirs_map(label_num);

  for (int i = 0; i < label_num; ++i) {
    for (auto& t : label_dirs[i]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);

      nbr_labels_set.insert(nbr_label);
      if (dir == Direction::kOut) {
        views[i].emplace_back(graph.GetOutgoingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      } else {
        CHECK(dir == Direction::kIn);
        views[i].emplace_back(graph.GetIncomingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      }
      label_dirs_map[i].emplace_back(nbr_label, edge_label, dir);
    }
  }

  if (nbr_labels_set.size() == 1) {
    auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(
        *nbr_labels_set.begin());
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, e.get_neighbor());
          }
        }
        ++csr_idx;
      }
    });
  } else {
    auto builder = state.getEdgeCollector<MLVertexColumn>();
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, nbr_label, e.get_neighbor());
          }
        }
        ++csr_idx;
      }
    });
  }
  return true;
}

template <typename EDATA_T, typename PRED_T>
inline bool expand_vertex_np_me_sp_optional(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphReadInterface::graph_view_t<EDATA_T>>> views(
      label_num);
  std::set<label_t> nbr_labels_set;
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
      label_dirs_map(label_num);

  for (int i = 0; i < label_num; ++i) {
    for (auto& t : label_dirs[i]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);

      nbr_labels_set.insert(nbr_label);
      if (dir == Direction::kOut) {
        views[i].emplace_back(graph.GetOutgoingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      } else {
        CHECK(dir == Direction::kIn);
        views[i].emplace_back(graph.GetIncomingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      }
      label_dirs_map[i].emplace_back(nbr_label, edge_label, dir);
    }
  }

  if (nbr_labels_set.size() == 1) {
    auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(
        *nbr_labels_set.begin());
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
      if (vid == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null(idx);
        return;
      }
      bool found = false;
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, e.get_neighbor());
            found = true;
          }
        }
        ++csr_idx;
      }
      if (!found) {
        builder.push_back_null(idx);
      }
    });
  } else {
    auto builder = state.getEdgeCollector<MLVertexColumn>();
    input.foreach_vertex(v_tag, [&](size_t idx, label_t l, vid_t vid) {
      if (vid == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null(idx);
        return;
      }
      size_t csr_idx = 0;
      bool found = false;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(idx, nbr_label, e.get_neighbor());
            found = true;
          }
        }
        ++csr_idx;
      }
      if (!found) {
        builder.push_back_null(idx);
      }
    });
  }
  return true;
}
/**
template <typename EDATA_T, typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_sp(
    const GraphReadInterface& graph, const MSVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  int label_num = label_dirs.size();
  std::vector<std::vector<GraphReadInterface::graph_view_t<EDATA_T>>> views(
      label_num);
  std::set<label_t> nbr_labels_set;
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
      label_dirs_map(label_num);

  for (int i = 0; i < label_num; ++i) {
    for (auto& t : label_dirs[i]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);

      nbr_labels_set.insert(nbr_label);
      if (dir == Direction::kOut) {
        views[i].emplace_back(graph.GetOutgoingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      } else {
        CHECK(dir == Direction::kIn);
        views[i].emplace_back(graph.GetIncomingGraphView<EDATA_T>(
            static_cast<label_t>(i), nbr_label, edge_label));
      }
      label_dirs_map[i].emplace_back(nbr_label, edge_label, dir);
    }
  }

  std::vector<size_t> offsets;
  std::shared_ptr<IContextColumn> col(nullptr);

  if (nbr_labels_set.size() == 1) {
    auto builder = SLVertexColumnBuilder::builder(*nbr_labels_set.begin());
    // not optimized for ms vertex column access
    LOG(INFO) << "not optimized for ms vertex column access";
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_opt(e.get_neighbor());
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish(nullptr);
  } else {
    auto builder = MLVertexColumnBuilder::builder();
    input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
      size_t csr_idx = 0;
      for (auto& view : views[l]) {
        label_t nbr_label = std::get<0>(label_dirs_map[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs_map[l][csr_idx]);
        Direction dir = std::get<2>(label_dirs_map[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto& e : es) {
          if (pred(l, vid, nbr_label, e.get_neighbor(), edge_label, dir,
                   e.get_data())) {
            builder.push_back_vertex({nbr_label, e.get_neighbor()});
            offsets.push_back(idx);
          }
        }
        ++csr_idx;
      }
    });
    col = builder.finish(nullptr);
  }
  return std::make_pair(col, std::move(offsets));
}
*/
template <typename PRED_T>
inline bool expand_vertex_np_me_mp(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  auto builder = state.getEdgeCollector<MLVertexColumn>();
  std::vector<size_t> offsets;
  input.foreach_vertex(v_tag, [&](size_t idx, label_t label, vid_t v) {
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it =
          (dir == Direction::kOut)
              ? (graph.GetOutEdgeIterator(label, v, nbr_label, edge_label))
              : (graph.GetInEdgeIterator(label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(label, v, nbr_label, nbr, edge_label, dir, it.GetData())) {
          builder.push_back_opt(idx, nbr_label, nbr);
        }
        it.Next();
      }
    }
  });
  return true;
}
/**
template <typename PRED_T>
inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_np_me_mp(
    const GraphReadInterface& graph, const MSVertexColumn& input,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred) {
  auto builder = MLVertexColumnBuilder::builder();
  std::vector<size_t> offsets;
  // not optimized for ms vertex access
  LOG(INFO) << "not optimized for ms vertex column access";
  input.foreach_vertex([&](size_t idx, label_t label, vid_t v) {
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it =
          (dir == Direction::kOut)
              ? (graph.GetOutEdgeIterator(label, v, nbr_label, edge_label))
              : (graph.GetInEdgeIterator(label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(label, v, nbr_label, nbr, edge_label, dir, it.GetData())) {
          builder.push_back_vertex({nbr_label, nbr});
          offsets.push_back(idx);
        }
        it.Next();
      }
    }
  });
  return std::make_pair(builder.finish(nullptr), std::move(offsets));
}
*/
template <typename PRED_T>
inline bool expand_vertex_optional_impl(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>&
        label_dirs,
    const PRED_T& pred, LocalEdgeExpandState& state) {
  auto builder = state.getEdgeCollector<MLVertexColumn>();
  input.foreach_vertex(v_tag, [&](size_t idx, label_t label, vid_t v) {
    if (v == std::numeric_limits<vid_t>::max()) {
      builder.push_back_null(idx);
      return;
    }
    bool has_nbr = false;
    for (auto& t : label_dirs[label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      auto it =
          (dir == Direction::kOut)
              ? (graph.GetOutEdgeIterator(label, v, nbr_label, edge_label))
              : (graph.GetInEdgeIterator(label, v, nbr_label, edge_label));
      while (it.IsValid()) {
        auto nbr = it.GetNeighbor();
        if (pred(label, v, nbr_label, nbr, edge_label, dir, it.GetData())) {
          builder.push_back_opt(idx, nbr_label, nbr);
          has_nbr = true;
        }
        it.Next();
      }
    }
    if (!has_nbr) {
      builder.push_back_null(idx);
    }
  });
  return true;
}

template <typename GPRED_T, typename EDATA_T>
struct GPredWrapper {
  GPredWrapper(const GPRED_T& gpred) : gpred_(gpred) {}

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label,
                         vid_t nbr_vid, label_t edge_label, Direction dir,
                         const EDATA_T& ed) const {
    Any edata = AnyConverter<EDATA_T>::to_any(ed);
    if (dir == Direction::kOut) {
      return gpred_(LabelTriplet(v_label, nbr_label, edge_label), v, nbr_vid,
                    edata, Direction::kOut);
    } else {
      return gpred_(LabelTriplet(nbr_label, v_label, edge_label), nbr_vid, v,
                    edata, Direction::kIn);
    }
  }

  const GPRED_T& gpred_;
};

template <typename GPRED_T>
struct GPredWrapper<GPRED_T, Any> {
  GPredWrapper(const GPRED_T& gpred) : gpred_(gpred) {}

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label,
                         vid_t nbr_vid, label_t edge_label, Direction dir,
                         const Any& edata) const {
    if (dir == Direction::kOut) {
      return gpred_(LabelTriplet(v_label, nbr_label, edge_label), v, nbr_vid,
                    edata, Direction::kOut);
    } else {
      return gpred_(LabelTriplet(nbr_label, v_label, edge_label), nbr_vid, v,
                    edata, Direction::kIn);
    }
  }

  const GPRED_T& gpred_;
};

template <typename VERTEX_COL_T, typename GPRED_T>
inline bool expand_vertex_impl(const GraphReadInterface& graph,
                               const DataChunk& input, int v_tag,
                               const std::vector<LabelTriplet>& labels,
                               Direction dir, const GPRED_T& gpred,
                               LocalEdgeExpandState& state) {
  if constexpr (std::is_same_v<VERTEX_COL_T, SLVertexColumn>) {
    label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    std::vector<PropertyType> ed_types;
    for (auto& triplet : labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
      if (triplet.dst_label == input_label &&
          ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
    }
    grape::DistinctSort(label_dirs);
    bool se = (label_dirs.size() == 1);
    bool sp = true;
    if (!se) {
      for (size_t k = 1; k < ed_types.size(); ++k) {
        if (ed_types[k] != ed_types[0]) {
          sp = false;
          break;
        }
      }
    }
    if (ed_types.empty()) {
      LOG(INFO) << "no edge property type in an edge(vertex) expand, fallback";
      // auto builder = MLVertexColumnBuilder::builder();
      // std::vector<size_t> offsets;
      // return std::make_pair(builder.finish(nullptr), std::move(offsets));
      return true;
    }
    if (sp && !check_exist_special_edge(graph, labels, dir)) {
      const PropertyType& ed_type = ed_types[0];
      if (ed_type == PropertyType::Empty()) {
        if (se) {
          return expand_vertex_np_se<grape::EmptyType,
                                     GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, v_tag, std::get<0>(label_dirs[0]),
              std::get<1>(label_dirs[0]), std::get<2>(label_dirs[0]),
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<
              grape::EmptyType, GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred), state);
        }
      } else if (ed_type == PropertyType::Int32()) {
        if (se) {
          return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
              graph, input, v_tag, std::get<0>(label_dirs[0]),
              std::get<1>(label_dirs[0]), std::get<2>(label_dirs[0]),
              GPredWrapper<GPRED_T, int>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int>(gpred), state);
        }
      } else if (ed_type == PropertyType::Int64()) {
        if (se) {
          return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
              graph, input, v_tag, std::get<0>(label_dirs[0]),
              std::get<1>(label_dirs[0]), std::get<2>(label_dirs[0]),
              GPredWrapper<GPRED_T, int64_t>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<int64_t,
                                        GPredWrapper<GPRED_T, int64_t>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int64_t>(gpred), state);
        }
      } else if (ed_type == PropertyType::Date()) {
        if (se) {
          return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, v_tag, std::get<0>(label_dirs[0]),
              std::get<1>(label_dirs[0]), std::get<2>(label_dirs[0]),
              GPredWrapper<GPRED_T, Date>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, Date>(gpred), state);
        }
      } else {
        LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
      }
    } else {
      LOG(INFO)
          << "different edge property type in an edge(vertex) expand, fallback";
    }
    return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
        graph, input, v_tag, label_dirs, GPredWrapper<GPRED_T, Any>(gpred),
        state);

  } else {
    const auto& input_labels = input.get_vertex_labels_set(v_tag);
    int label_num = graph.schema().vertex_label_num();
    std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
        label_dirs(label_num);
    std::vector<PropertyType> ed_types;
    for (auto& triplet : labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if ((input_labels.find(triplet.src_label) != input_labels.end()) &&
          ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
        label_dirs[triplet.src_label].emplace_back(
            triplet.dst_label, triplet.edge_label, Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
      if ((input_labels.find(triplet.dst_label) != input_labels.end()) &&
          ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
        label_dirs[triplet.dst_label].emplace_back(
            triplet.src_label, triplet.edge_label, Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
    }
    bool se = true;
    for (auto& vec : label_dirs) {
      grape::DistinctSort(vec);
      if (vec.size() > 1) {
        se = false;
      }
    }
    bool sp = true;
    for (size_t k = 1; k < ed_types.size(); ++k) {
      if (ed_types[k] != ed_types[0]) {
        sp = false;
        break;
      }
    }
    if (ed_types.size() == 0) {
      LOG(INFO) << "no edge property type in an edge(vertex) expand, fallback";
      return true;
    }
    if (sp && !check_exist_special_edge(graph, labels, dir)) {
      const PropertyType& ed_type = ed_types[0];
      if (ed_type == PropertyType::Empty()) {
        if (se) {
          return expand_vertex_np_se<grape::EmptyType,
                                     GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<
              grape::EmptyType, GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred), state);
        }
      } else if (ed_type == PropertyType::Int32()) {
        if (se) {
          return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int>(gpred), state);
        }
      } else if (ed_type == PropertyType::Int64()) {
        if (se) {
          return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int64_t>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<int64_t,
                                        GPredWrapper<GPRED_T, int64_t>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, int64_t>(gpred), state);
        }
      } else if (ed_type == PropertyType::Date()) {
        if (se) {
          return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, Date>(gpred), state);
        } else {
          return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, v_tag, label_dirs,
              GPredWrapper<GPRED_T, Date>(gpred), state);
        }
      } else {
        LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
      }
    } else {
      LOG(INFO)
          << "different edge property type in an edge(vertex) expand, fallback";
    }
    return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
        graph, input, v_tag, label_dirs, GPredWrapper<GPRED_T, Any>(gpred),
        state);
  }
}
/**  template <typename GPRED_T>
  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
  expand_vertex_impl(const GraphReadInterface& graph,
                     const MSVertexColumn& input,
                     const std::vector<LabelTriplet>& labels, Direction dir,
                     const GPRED_T& gpred) {
    const std::set<label_t>& input_labels = input.get_labels_set();
    int label_num = graph.schema().vertex_label_num();
    std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
        label_dirs(label_num);
    std::vector<PropertyType> ed_types;
    for (auto& triplet : labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if ((input_labels.find(triplet.src_label) != input_labels.end()) &&
          ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
        label_dirs[triplet.src_label].emplace_back(
            triplet.dst_label, triplet.edge_label, Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
      if ((input_labels.find(triplet.dst_label) != input_labels.end()) &&
          ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
        label_dirs[triplet.dst_label].emplace_back(
            triplet.src_label, triplet.edge_label, Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
    }
    bool se = true;
    for (auto& vec : label_dirs) {
      grape::DistinctSort(vec);
      if (vec.size() > 1) {
        se = false;
      }
    }
    bool sp = true;
    for (size_t k = 1; k < ed_types.size(); ++k) {
      if (ed_types[k] != ed_types[0]) {
        sp = false;
        break;
      }
    }
    if (ed_types.empty()) {
      LOG(INFO) << "no edge property type in an edge(vertex) expand,
  fallback"; auto builder = MLVertexColumnBuilder::builder(); return
  std::make_pair(builder.finish(nullptr), std::vector<size_t>());
    }
    if (sp && !check_exist_special_edge(graph, labels, dir)) {
      const PropertyType& ed_type = ed_types[0];
      if (ed_type == PropertyType::Empty()) {
        if (se) {
          return expand_vertex_np_se<grape::EmptyType,
                                     GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, label_dirs,
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
        } else {
          return expand_vertex_np_me_sp<
              grape::EmptyType, GPredWrapper<GPRED_T, grape::EmptyType>>(
              graph, input, label_dirs,
              GPredWrapper<GPRED_T, grape::EmptyType>(gpred));
        }
      } else if (ed_type == PropertyType::Int32()) {
        if (se) {
          return expand_vertex_np_se<int, GPredWrapper<GPRED_T, int>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
        } else {
          return expand_vertex_np_me_sp<int, GPredWrapper<GPRED_T, int>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T, int>(gpred));
        }
      } else if (ed_type == PropertyType::Int64()) {
        if (se) {
          return expand_vertex_np_se<int64_t, GPredWrapper<GPRED_T, int64_t>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T,
  int64_t>(gpred)); } else { return expand_vertex_np_me_sp<int64_t,
                                        GPredWrapper<GPRED_T, int64_t>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T,
  int64_t>(gpred));
        }
      } else if (ed_type == PropertyType::Date()) {
        if (se) {
          return expand_vertex_np_se<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
        } else {
          return expand_vertex_np_me_sp<Date, GPredWrapper<GPRED_T, Date>>(
              graph, input, label_dirs, GPredWrapper<GPRED_T, Date>(gpred));
        }
      } else {
        LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
      }
    } else {
      LOG(INFO)
          << "different edge property type in an edge(vertex) expand,
  fallback";
    }
    return expand_vertex_np_me_mp<GPredWrapper<GPRED_T, Any>>(
        graph, input, label_dirs, GPredWrapper<GPRED_T, Any>(gpred));
  }
  */

template <typename VERTEX_COL_T>
bool expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state) = delete;

template <typename VERTEX_COL_T>
bool expand_vertex_without_predicate_optional_impl(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state) = delete;

template <>
bool expand_vertex_without_predicate_impl<SLVertexColumn>(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state);
template <>
bool expand_vertex_without_predicate_impl<MLVertexColumn>(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state);

template <>
bool expand_vertex_without_predicate_optional_impl<SLVertexColumn>(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state);

template <>
bool expand_vertex_without_predicate_optional_impl<MLVertexColumn>(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir,
    LocalEdgeExpandState& state);

template <typename EDATA_T, typename PRED_T>
inline bool expand_edge_ep_se(const GraphReadInterface& graph,
                              const DataChunk& input, int v_tag,
                              label_t nbr_label, label_t edge_label,
                              Direction dir, const PropertyType& prop_type,
                              const PRED_T& pred, LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  CHECK((dir == Direction::kIn) || (dir == Direction::kOut));
  LabelTriplet triplet(dir == Direction::kIn ? nbr_label : input_label,
                       dir == Direction::kIn ? input_label : nbr_label,
                       edge_label);
  auto builder = state.getEdgeCollector<SDSLEdgeColumn, Direction, LabelTriplet,
                                        PropertyType>(dir, triplet, prop_type);
  // SDSLEdgeColumnBuilderBeta<EDATA_T> builder(dir, triplet, prop_type);
  std::vector<size_t> offsets;
  if (dir == Direction::kIn) {
    GraphReadInterface::graph_view_t<EDATA_T> view =
        graph.GetIncomingGraphView<EDATA_T>(input_label, nbr_label, edge_label);
    input.foreach_vertex(v_tag, [&](size_t idx, label_t v_label, vid_t v) {
      auto es = view.get_edges(v);
      for (auto& e : es) {
        Any edata = AnyConverter<EDATA_T>::to_any(e.get_data());
        if (pred(triplet, e.get_neighbor(), v, edata, dir, idx)) {
          builder.push_back_opt(idx, e.get_neighbor(), v, e.get_data());
        }
      }
    });
  } else if (dir == Direction::kOut) {
    GraphReadInterface::graph_view_t<EDATA_T> view =
        graph.GetOutgoingGraphView<EDATA_T>(input_label, nbr_label, edge_label);
    input.foreach_vertex(v_tag, [&](size_t idx, label_t v_label, vid_t v) {
      auto es = view.get_edges(v);
      for (auto& e : es) {
        Any edata = AnyConverter<EDATA_T>::to_any(e.get_data());
        if (pred(triplet, v, e.get_neighbor(), edata, dir, idx)) {
          builder.push_back_opt(idx, v, e.get_neighbor(), e.get_data());
        }
      }
    });
  } else {
    // We will handle edge_expand with both direction outside this function,
    // in EdgeExpand::expand_edge.
    return false;
  }

  return true;
}

template <typename PRED_T>
bool expand_edge_impl(const GraphReadInterface& graph, const DataChunk& input,
                      int v_tag, const LabelTriplet& triplet,
                      const PRED_T& pred, Direction dir,
                      LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  std::tuple<label_t, label_t, Direction> label_dir;
  CHECK(graph.schema().exist(triplet.src_label, triplet.dst_label,
                             triplet.edge_label));
  if (dir == Direction::kOut) {
    CHECK(triplet.src_label == input_label);
    std::get<0>(label_dir) = triplet.dst_label;
  } else if (dir == Direction::kIn) {
    CHECK(triplet.dst_label == input_label);
    std::get<0>(label_dir) = triplet.src_label;
  } else {
    return true;
  }
  std::get<1>(label_dir) = triplet.edge_label;
  std::get<2>(label_dir) = dir;

  const auto& properties = graph.schema().get_edge_properties(
      triplet.src_label, triplet.dst_label, triplet.edge_label);
  if (properties.empty()) {
    return expand_edge_ep_se<grape::EmptyType, PRED_T>(
        graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
        std::get<2>(label_dir), PropertyType::Empty(), pred, state);
  } else if (properties.size() == 1) {
    const PropertyType& ed_type = properties[0];
    if (ed_type == PropertyType::Int32()) {
      return expand_edge_ep_se<int, PRED_T>(
          graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred, state);
    } else if (ed_type == PropertyType::Int64()) {
      return expand_edge_ep_se<int64_t, PRED_T>(
          graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred, state);
    } else if (ed_type == PropertyType::Date()) {
      return expand_edge_ep_se<Date, PRED_T>(
          graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred, state);
    } else if (ed_type == PropertyType::Double()) {
      return expand_edge_ep_se<double, PRED_T>(
          graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred, state);
    } else if (ed_type == PropertyType::StringView()) {
      return expand_edge_ep_se<std::string_view, PRED_T>(
          graph, input, v_tag, std::get<0>(label_dir), std::get<1>(label_dir),
          std::get<2>(label_dir), ed_type, pred, state);
    } else {
      LOG(INFO) << "type - " << ed_type << " - not implemented, fallback";
    }
  } else {
    LOG(INFO) << "multiple properties not supported, fallback";
  }
  return false;
}

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_IMPL_H_