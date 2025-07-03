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

#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/path_expand_impl.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
bl::result<void> iterative_expand_vertex(const GraphReadInterface& graph,
                                         const DataChunk& input, int v_tag,
                                         label_t edge_label, Direction dir,
                                         int lower, int upper,
                                         LocalEdgeExpandState& state) {
  int input_label = *input.get_vertex_labels_set(v_tag).begin();
  auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(input_label);
  if (upper == lower) {
    return bl::result<void>();
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      builder.push_back_opt(index, v);
    });
    return bl::result<void>();
  }

  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      output_list.emplace_back(v, index);
    });
  }

  int depth = 0;
  if (dir == Direction::kOut) {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);
            auto it = graph.GetOutEdgeIterator(input_label, pair.first,
                                               input_label, edge_label);
            while (it.IsValid()) {
              output_list.emplace_back(it.GetNeighbor(), pair.second);
              it.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it = graph.GetOutEdgeIterator(input_label, pair.first,
                                             input_label, edge_label);
          while (it.IsValid()) {
            output_list.emplace_back(it.GetNeighbor(), pair.second);
            it.Next();
          }
        }
      }
      ++depth;
    }
  } else if (dir == Direction::kIn) {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);

            auto it = graph.GetInEdgeIterator(input_label, pair.first,
                                              input_label, edge_label);
            while (it.IsValid()) {
              output_list.emplace_back(it.GetNeighbor(), pair.second);
              it.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it = graph.GetInEdgeIterator(input_label, pair.first,
                                            input_label, edge_label);
          while (it.IsValid()) {
            output_list.emplace_back(it.GetNeighbor(), pair.second);
            it.Next();
          }
        }
      }
      ++depth;
    }
  } else {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.second, pair.first);

            auto it0 = graph.GetInEdgeIterator(input_label, pair.first,
                                               input_label, edge_label);
            while (it0.IsValid()) {
              output_list.emplace_back(it0.GetNeighbor(), pair.second);
              it0.Next();
            }
            auto it1 = graph.GetOutEdgeIterator(input_label, pair.first,
                                                input_label, edge_label);
            while (it1.IsValid()) {
              output_list.emplace_back(it1.GetNeighbor(), pair.second);
              it1.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it0 = graph.GetInEdgeIterator(input_label, pair.first,
                                             input_label, edge_label);
          while (it0.IsValid()) {
            output_list.emplace_back(it0.GetNeighbor(), pair.second);
            it0.Next();
          }
          auto it1 = graph.GetOutEdgeIterator(input_label, pair.first,
                                              input_label, edge_label);
          while (it1.IsValid()) {
            output_list.emplace_back(it1.GetNeighbor(), pair.second);
            it1.Next();
          }
        }
      }
      ++depth;
    }
  }

  return bl::result<void>();
}

template <typename EDATA_T>
bl::result<void> iterative_expand_vertex_on_graph_view(
    const GraphReadInterface::graph_view_t<EDATA_T>& view,
    const DataChunk& input, int v_tag, int lower, int upper,
    LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return bl::result<void>();
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      builder.push_back_opt(index, v);
    });
    return bl::result<void>();
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      output_list.emplace_back(v, index);
    });
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.second, pair.first);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.second, pair.first);

          auto es = view.get_edges(pair.first);
          for (auto& e : es) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto es = view.get_edges(pair.first);
        for (auto& e : es) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
      }
    }
    ++depth;
  }

  return bl::result<void>();
}

template <typename EDATA_T>
bl::result<void> iterative_expand_vertex_on_dual_graph_view(
    const GraphReadInterface::graph_view_t<EDATA_T>& iview,
    const GraphReadInterface::graph_view_t<EDATA_T>& oview,
    const DataChunk& input, int v_tag, int lower, int upper,
    LocalEdgeExpandState& state) {
  label_t input_label = *input.get_vertex_labels_set(v_tag).begin();
  auto builder = state.getEdgeCollector<SLVertexColumn, label_t>(input_label);
  if (upper == lower) {
    return bl::result<void>();
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      builder.push_back_opt(index, v);
    });
    return bl::result<void>();
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    input.foreach_vertex(v_tag, [&](size_t index, label_t label, vid_t v) {
      output_list.emplace_back(v, index);
    });
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.second, pair.first);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.second, pair.first);
          auto ies = iview.get_edges(pair.first);
          for (auto& e : ies) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
          auto oes = oview.get_edges(pair.first);
          for (auto& e : oes) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto ies = iview.get_edges(pair.first);
        for (auto& e : ies) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
        auto oes = oview.get_edges(pair.first);
        for (auto& e : oes) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
      }
    }
    ++depth;
  }

  return bl::result<void>();
}

bl::result<void> path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const DataChunk& input, int v_tag,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper, LocalEdgeExpandState& state) {
  if (labels.size() == 1) {
    label_t v_label = *input.get_vertex_labels_set(v_tag).begin();
    if (labels[0].src_label == labels[0].dst_label &&
        labels[0].src_label == v_label) {
      label_t v_label = labels[0].src_label;
      label_t e_label = labels[0].edge_label;
      auto property = EdgeExpand::parse_edge_property_type(
          graph.schema(), labels[0].src_label, labels[0].dst_label,
          labels[0].edge_label);
      if (dir == Direction::kBoth) {
        if (property == PropertyType::Empty()) {
          auto iview = graph.GetIncomingGraphView<grape::EmptyType>(
              v_label, v_label, e_label);
          auto oview = graph.GetOutgoingGraphView<grape::EmptyType>(
              v_label, v_label, e_label);
          return iterative_expand_vertex_on_dual_graph_view(
              iview, oview, input, v_tag, lower, upper, state);
        } else if (property == PropertyType::Int32()) {
          auto iview =
              graph.GetIncomingGraphView<int>(v_label, v_label, e_label);
          auto oview =
              graph.GetOutgoingGraphView<int>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_dual_graph_view(
              iview, oview, input, v_tag, lower, upper, state);
        } else if (property == PropertyType::Int64()) {
          auto iview =
              graph.GetIncomingGraphView<int64_t>(v_label, v_label, e_label);
          auto oview =
              graph.GetOutgoingGraphView<int64_t>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_dual_graph_view(
              iview, oview, input, v_tag, lower, upper, state);
        } else if (property == PropertyType::Date()) {
          auto iview =
              graph.GetIncomingGraphView<Date>(v_label, v_label, e_label);
          auto oview =
              graph.GetOutgoingGraphView<Date>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_dual_graph_view(
              iview, oview, input, v_tag, lower, upper, state);
        }
      } else if (dir == Direction::kIn) {
        if (property == PropertyType::Empty()) {
          auto iview = graph.GetIncomingGraphView<grape::EmptyType>(
              v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(iview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Int32()) {
          auto iview =
              graph.GetIncomingGraphView<int>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(iview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Int64()) {
          auto iview =
              graph.GetIncomingGraphView<int64_t>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(iview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Date()) {
          auto iview =
              graph.GetIncomingGraphView<Date>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(iview, input, v_tag,
                                                       lower, upper, state);
        }
      } else if (dir == Direction::kOut) {
        if (property == PropertyType::Empty()) {
          auto oview = graph.GetOutgoingGraphView<grape::EmptyType>(
              v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(oview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Int32()) {
          auto oview =
              graph.GetOutgoingGraphView<int>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(oview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Int64()) {
          auto oview =
              graph.GetOutgoingGraphView<int64_t>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(oview, input, v_tag,
                                                       lower, upper, state);
        } else if (property == PropertyType::Date()) {
          auto oview =
              graph.GetOutgoingGraphView<Date>(v_label, v_label, e_label);
          return iterative_expand_vertex_on_graph_view(oview, input, v_tag,
                                                       lower, upper, state);
        }
      }

      return iterative_expand_vertex(graph, input, v_tag, e_label, dir, lower,
                                     upper, state);
    }
  }
  LOG(FATAL) << "not support path expand with multiple edge types";
  RETURN_UNSUPPORTED_ERROR("not support path expand with multiple edge types");
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs