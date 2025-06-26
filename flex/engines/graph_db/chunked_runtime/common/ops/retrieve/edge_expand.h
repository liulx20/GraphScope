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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand_impl.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand_state.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/engines/graph_db/runtime/utils/params.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::EdgeExpandParams;
using gs::runtime::GraphReadInterface;

class EdgeExpand {
 public:
  inline static PropertyType parse_edge_property_type(const gs::Schema& schema,
                                                      label_t src_label,
                                                      label_t dst_label,
                                                      label_t edge_label) {
    auto& props = schema.get_edge_properties(src_label, dst_label, edge_label);
    PropertyType pt = PropertyType::kEmpty;
    if (!props.empty()) {
      pt = props[0];
    }
    if (props.size() > 1) {
      pt = PropertyType::kRecordView;
    }
    return pt;
  }

  template <typename PRED_T>
  static bl::result<void> expand_edge(const GraphReadInterface& graph,
                                      const DataChunk& chunk,
                                      const EdgeExpandParams& params,
                                      const PRED_T& pred,
                                      LocalEdgeExpandState& state) {
    if (params.is_optional) {
      LOG(ERROR) << "not support optional edge expand";
      RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
    }
    if (params.labels.size() == 1) {
      if (params.dir == Direction::kIn) {
        label_t output_vertex_label = params.labels[0].src_label;
        label_t edge_label = params.labels[0].edge_label;
        PropertyType pt = parse_edge_property_type(
            graph.schema(), params.labels[0].src_label,
            params.labels[0].dst_label, params.labels[0].edge_label);
        auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                                LabelTriplet, PropertyType>(
            params.dir, params.labels[0], std::move(pt));
        chunk.foreach_vertex(
            params.v_tag, [&](size_t index, label_t label, vid_t v) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, output_vertex_label, edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                if (pred(params.labels[0], nbr, v, ie_iter.GetData(),
                         Direction::kIn)) {
                  assert(ie_iter.GetData().type == pt);
                  collector.push_back_opt(index, nbr, v, ie_iter.GetData());
                }
                ie_iter.Next();
              }
            });
      } else if (params.dir == Direction::kOut) {
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;
        PropertyType pt = parse_edge_property_type(
            graph.schema(), params.labels[0].src_label,
            params.labels[0].dst_label, params.labels[0].edge_label);
        auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                                LabelTriplet, PropertyType>(
            params.dir, params.labels[0], pt);
        chunk.foreach_vertex(
            params.v_tag, [&](size_t index, label_t label, vid_t v) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, output_vertex_label, edge_label);
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                if (pred(params.labels[0], v, nbr, oe_iter.GetData(),
                         Direction::kOut)) {
                  assert(oe_iter.GetData().type == pt);
                  collector.push_back_opt(index, v, nbr, oe_iter.GetData());
                }
                oe_iter.Next();
              }
            });
      } else {
        auto src_label = params.labels[0].src_label;
        auto dst_label = params.labels[0].dst_label;
        auto edge_label = params.labels[0].edge_label;
        PropertyType pt = parse_edge_property_type(graph.schema(), src_label,
                                                   dst_label, edge_label);
        auto collector =
            state.getEdgeCollector<BDSLEdgeColumn, LabelTriplet, PropertyType>(
                params.labels[0], pt);

        chunk.foreach_vertex(
            params.v_tag, [&](size_t index, label_t label, vid_t v) {
              if (label == src_label) {
                auto oe_iter =
                    graph.GetOutEdgeIterator(label, v, dst_label, edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  if (pred(params.labels[0], v, nbr, oe_iter.GetData(),
                           Direction::kOut)) {
                    assert(oe_iter.GetData().type == pt);
                    collector.push_back_opt(index, v, nbr, oe_iter.GetData(),
                                            Direction::kOut);
                  }
                  oe_iter.Next();
                }
              }
              if (label == dst_label) {
                auto ie_iter =
                    graph.GetInEdgeIterator(label, v, src_label, edge_label);
                while (ie_iter.IsValid()) {
                  auto nbr = ie_iter.GetNeighbor();
                  if (pred(params.labels[0], nbr, v, ie_iter.GetData(),
                           Direction::kIn)) {
                    assert(ie_iter.GetData().type == pt);
                    collector.push_back_opt(index, nbr, v, ie_iter.GetData(),
                                            Direction::kIn);
                  }
                  ie_iter.Next();
                }
              }
            });
      }
      return bl::result<void>();
    } else {
      std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
      if (params.dir == Direction::kBoth) {
        for (const auto& triplet : params.labels) {
          auto pt =
              parse_edge_property_type(graph.schema(), triplet.src_label,
                                       triplet.dst_label, triplet.edge_label);
          label_props.emplace_back(triplet, pt);
        }
        auto collector = state.getEdgeCollector<
            BDMLEdgeColumn, std::vector<std::pair<LabelTriplet, PropertyType>>>(
            label_props);
        chunk.foreach_vertex(params.v_tag, [&](size_t index, label_t label,
                                               vid_t v) {
          for (auto& label_prop : label_props) {
            auto& triplet = label_prop.first;
            if (label == triplet.src_label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut)) {
                  assert(oe_iter.GetData().type == label_prop.second);
                  collector.push_back_opt(index, triplet, v, nbr,
                                          oe_iter.GetData(), Direction::kOut);
                }
                oe_iter.Next();
              }
            }
            if (label == triplet.dst_label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn)) {
                  assert(ie_iter.GetData().type == label_prop.second);
                  collector.push_back_opt(index, triplet, nbr, v,
                                          ie_iter.GetData(), Direction::kIn);
                }
                ie_iter.Next();
              }
            }
          }
        });
      } else if (params.dir == Direction::kOut) {
        for (auto& triplet : params.labels) {
          auto pt =
              parse_edge_property_type(graph.schema(), triplet.src_label,
                                       triplet.dst_label, triplet.edge_label);
          label_props.emplace_back(triplet, pt);
        }
        auto collector = state.getEdgeCollector<
            SDMLEdgeColumn, Direction,
            std::vector<std::pair<LabelTriplet, PropertyType>>>(Direction::kOut,
                                                                label_props);
        chunk.foreach_vertex(params.v_tag, [&](size_t index, label_t label,
                                               vid_t v) {
          for (auto& label_prop : label_props) {
            auto& triplet = label_prop.first;
            if (label != triplet.src_label)
              continue;
            auto oe_iter = graph.GetOutEdgeIterator(label, v, triplet.dst_label,
                                                    triplet.edge_label);
            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut)) {
                assert(oe_iter.GetData().type == label_prop.second);
                collector.push_back_opt(index, triplet, v, nbr,
                                        oe_iter.GetData());
              }
              oe_iter.Next();
            }
          }
        });
      } else {
        for (auto& triplet : params.labels) {
          auto pt =
              parse_edge_property_type(graph.schema(), triplet.src_label,
                                       triplet.dst_label, triplet.edge_label);
          label_props.emplace_back(triplet, pt);
        }
        auto collector = state.getEdgeCollector<
            SDMLEdgeColumn, Direction,
            std::vector<std::pair<LabelTriplet, PropertyType>>>(Direction::kIn,
                                                                label_props);
        chunk.foreach_vertex(params.v_tag, [&](size_t index, label_t label,
                                               vid_t v) {
          for (auto& label_prop : label_props) {
            auto& triplet = label_prop.first;
            if (label != triplet.src_label)
              continue;
            auto ie_iter = graph.GetInEdgeIterator(label, v, triplet.src_label,
                                                   triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              if (pred(triplet, v, nbr, ie_iter.GetData(), Direction::kIn)) {
                assert(oe_iter.GetData().type == label_prop.second);
                collector.push_back_opt(index, triplet, v, nbr,
                                        ie_iter.GetData());
              }
              ie_iter.Next();
            }
          }
        });
      }
      return bl::result<void>();
    }
    LOG(ERROR) << "expand edge not support";
    RETURN_UNSUPPORTED_ERROR("expand edge not support");
  }

  static bl::result<void> expand_edge_with_special_edge_predicate(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const EdgeExpandParams& params, const gs::runtime::SPEdgePredicate& pred,
      LocalEdgeExpandState& state);

  static bl::result<void> expand_edge_without_predicate(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const EdgeExpandParams& params, LocalEdgeExpandState& state);

  template <typename PRED_T>
  static bl::result<void> expand_vertex(const GraphReadInterface& graph,
                                        const DataChunk& ctx,
                                        const EdgeExpandParams& params,
                                        const PRED_T& pred,
                                        LocalEdgeExpandState& state) {
    if (params.is_optional) {
      LOG(ERROR) << "not support optional edge expand with predicate";
      RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
    }
    VertexColumnType input_vertex_list_type =
        ctx.get_vertex_column_type(params.v_tag);

    if (input_vertex_list_type == VertexColumnType::kSingle) {
      expand_vertex_impl<SLVertexColumn, PRED_T>(
          graph, ctx, params.v_tag, params.labels, params.dir, pred, state);
      return bl::result<void>();
    } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
      expand_vertex_impl<MLVertexColumn, PRED_T>(
          graph, ctx, params.v_tag, params.labels, params.dir, pred, state);
      return bl::result<void>();
    } /*else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } */
    else {
      LOG(ERROR) << "not support vertex column type "
                 << static_cast<int>(input_vertex_list_type);
      RETURN_UNSUPPORTED_ERROR(
          "not support vertex column type " +
          std::to_string(static_cast<int>(input_vertex_list_type)));
    }
  }

  static bl::result<void> expand_vertex_ep_lt(const GraphReadInterface& graph,
                                              const DataChunk& ctx,
                                              const EdgeExpandParams& params,
                                              const std::string& ep_val,
                                              LocalEdgeExpandState& state);
  static bl::result<void> expand_vertex_ep_gt(const GraphReadInterface& graph,
                                              const DataChunk& ctx,
                                              const EdgeExpandParams& params,
                                              const std::string& ep_val,
                                              LocalEdgeExpandState& state);
  template <typename PRED_T>
  struct SPVPWrapper {
    SPVPWrapper(const PRED_T& pred) : pred_(pred) {}

    inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& edata, Direction dir) const {
      if (dir == Direction::kOut) {
        return pred_(label.dst_label, dst);
      } else {
        return pred_(label.src_label, src);
      }
    }

    const PRED_T& pred_;
  };

  static bl::result<void> expand_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const EdgeExpandParams& params,
      const gs::runtime::SPVertexPredicate& pred, LocalEdgeExpandState& state);

  static bl::result<void> expand_vertex_without_predicate(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const EdgeExpandParams& params, LocalEdgeExpandState& state);

  template <typename T1, typename T2, typename T3>
  static bl::result<void> tc(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const std::array<std::tuple<label_t, label_t, label_t, Direction>, 3>&
          labels,
      int input_tag, int alias1, int alias2, bool LT, const std::string& val,
      LocalEdgeExpandState& state) {
    if (ctx.get_vertex_column_type(input_tag) != VertexColumnType::kSingle) {
      RETURN_UNSUPPORTED_ERROR(
          "Unsupported input for triangle counting, only single vertex column");
    }
    /*auto casted_input_vertex_list =
        std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
    label_t input_label = casted_input_vertex_list->label();
    auto dir0 = std::get<3>(labels[0]);
    auto dir1 = std::get<3>(labels[1]);
    auto dir2 = std::get<3>(labels[2]);
    auto d0_nbr_label = std::get<1>(labels[0]);
    auto d0_e_label = std::get<2>(labels[0]);
    auto csr0 = (dir0 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T1>(input_label, d0_nbr_label,
                                                     d0_e_label)
                    : graph.GetIncomingGraphView<T1>(input_label, d0_nbr_label,
                                                     d0_e_label);
    auto d1_nbr_label = std::get<1>(labels[1]);
    auto d1_e_label = std::get<2>(labels[1]);
    auto csr1 = (dir1 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T2>(input_label, d1_nbr_label,
                                                     d1_e_label)
                    : graph.GetIncomingGraphView<T2>(input_label, d1_nbr_label,
                                                     d1_e_label);
    auto d2_nbr_label = std::get<1>(labels[2]);
    auto d2_e_label = std::get<2>(labels[2]);
    auto csr2 = (dir2 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T3>(d1_nbr_label, d2_nbr_label,
                                                     d2_e_label)
                    : graph.GetIncomingGraphView<T3>(d1_nbr_label, d2_nbr_label,
                                                     d2_e_label);

    T1 param = TypedConverter<T1>::typed_from_string(val);

    auto builder1 = SLVertexColumnBuilder::builder(d1_nbr_label);
    auto builder2 = SLVertexColumnBuilder::builder(d2_nbr_label);
    std::vector<size_t> offsets;

    size_t idx = 0;
    static thread_local GraphReadInterface::vertex_array_t<bool> d0_set;
    static thread_local std::vector<vid_t> d0_vec;

    d0_set.Init(graph.GetVertexSet(d0_nbr_label), false);
    for (auto v : casted_input_vertex_list->vertices()) {
      if (LT) {
        csr0.foreach_edges_lt(v, param, [&](vid_t u, const Date& date) {
          d0_set[u] = true;
          d0_vec.push_back(u);
        });
      } else {
        csr0.foreach_edges_gt(v, param, [&](vid_t u, const Date& date) {
          d0_set[u] = true;
          d0_vec.push_back(u);
        });
      }
      for (auto& e1 : csr1.get_edges(v)) {
        auto nbr1 = e1.get_neighbor();
        for (auto& e2 : csr2.get_edges(nbr1)) {
          auto nbr2 = e2.get_neighbor();
          if (d0_set[nbr2]) {
            builder1.push_back_opt(nbr1);
            builder2.push_back_opt(nbr2);
            offsets.push_back(idx);
          }
        }
      }
      for (auto u : d0_vec) {
        d0_set[u] = false;
      }
      d0_vec.clear();
      ++idx;
    }

    std::shared_ptr<IContextColumn> col1 = builder1.finish(nullptr);
    std::shared_ptr<IContextColumn> col2 = builder2.finish(nullptr);
    ctx.set_with_reshuffle(alias1, col1, offsets);
    ctx.set(alias2, col2);
    return ctx;*/
    return bl::result<void>();
  }
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_H_