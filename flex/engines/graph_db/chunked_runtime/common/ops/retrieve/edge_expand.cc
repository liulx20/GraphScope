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

#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {

namespace chunked_runtime {
namespace ops {

static std::vector<LabelTriplet> get_expand_label_set(
    const GraphReadInterface& graph,
    const std::unordered_set<label_t>& label_set,
    const std::vector<LabelTriplet>& labels, Direction dir) {
  std::vector<LabelTriplet> label_triplets;
  if (dir == Direction::kOut) {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.src_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  } else if (dir == Direction::kIn) {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.dst_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  } else {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.src_label) ||
          label_set.count(triplet.dst_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  }
  return label_triplets;
}

using gs::runtime::SPEdgePredicate;
template <typename T>
static bl::result<void> _expand_edge_with_special_edge_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const gs::runtime::EdgeExpandParams& params, const SPEdgePredicate& pred,
    LocalEdgeExpandState& state) {
  if (pred.type() == gs::runtime::SPPredicateType::kPropertyGT) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyGTPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyGTPredicate<T>&>(pred),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyLT) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyLTPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyLTPredicate<T>&>(pred),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyEQ) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyEQPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyEQPredicate<T>&>(pred),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyNE) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyNEPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyNEPredicate<T>&>(pred),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyLE) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyLEPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyLEPredicate<T>&>(pred),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyGE) {
    return EdgeExpand::expand_edge<gs::runtime::EdgePropertyGEPredicate<T>>(
        graph, ctx, params,
        dynamic_cast<const gs::runtime::EdgePropertyGEPredicate<T>&>(pred),
        state);
  } else {
    LOG(ERROR) << "not support edge property type "
               << static_cast<int>(pred.type());
  }
  RETURN_UNSUPPORTED_ERROR("not support edge property type " +
                           std::to_string(static_cast<int>(pred.type())));
}

bl::result<void> EdgeExpand::expand_edge_with_special_edge_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, const gs::runtime::SPEdgePredicate& pred,
    LocalEdgeExpandState& state) {
  if (params.is_optional) {
    LOG(ERROR) << "not support optional edge expand";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }
  if (pred.data_type() == RTAnyType::kI64Value) {
    return _expand_edge_with_special_edge_predicate<int64_t>(graph, ctx, params,
                                                             pred, state);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _expand_edge_with_special_edge_predicate<int32_t>(graph, ctx, params,
                                                             pred, state);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_edge_with_special_edge_predicate<double>(graph, ctx, params,
                                                            pred, state);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _expand_edge_with_special_edge_predicate<std::string_view>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _expand_edge_with_special_edge_predicate<Date>(graph, ctx, params,
                                                          pred, state);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_edge_with_special_edge_predicate<double>(graph, ctx, params,
                                                            pred, state);
  } else {
    LOG(ERROR) << "not support edge property type "
               << static_cast<int>(pred.data_type());
    RETURN_UNSUPPORTED_ERROR(
        "not support edge property type " +
        std::to_string(static_cast<int>(pred.data_type())));
  }
  RETURN_UNSUPPORTED_ERROR("not support edge property type");
}

static bl::result<void> expand_edge_without_predicate_optional_impl(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, LocalEdgeExpandState& state) {
  std::vector<size_t> shuffle_offset;
  // has only one label
  if (params.labels.size() == 1) {
    // both direction and src_label == dst_label
    if (params.dir == Direction::kBoth &&
        params.labels[0].src_label == params.labels[0].dst_label) {
      if (ctx.is_optional(params.v_tag)) {
        LOG(ERROR) << "not support optional vertex column as input currently";
        RETURN_UNSUPPORTED_ERROR(
            "not support optional vertex column as input currently");
      }
      const auto& triplet = params.labels[0];
      auto pt = EdgeExpand::parse_edge_property_type(
          graph.schema(), triplet.src_label, triplet.dst_label,
          triplet.edge_label);
      auto builder =
          state.getEdgeCollector<BDSLEdgeColumn, LabelTriplet, PropertyType>(
              triplet, pt);
      // BDSLEdgeColumnBuilder::optional_builder(triplet, pt);
      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            bool has_edge = false;
            if (label == triplet.src_label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                builder.push_back_opt(index, v, nbr, oe_iter.GetData(),
                                      Direction::kOut);
                has_edge = true;
                oe_iter.Next();
              }
            }
            if (label == triplet.dst_label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                builder.push_back_opt(index, nbr, v, ie_iter.GetData(),
                                      Direction::kIn);
                has_edge = true;
                ie_iter.Next();
              }
            }
            if (!has_edge) {
              builder.push_back_null(index);
            }
          });
      return bl::result<void>();
    } else if (params.dir == Direction::kOut) {
      if (ctx.is_optional(params.v_tag)) {
        LOG(ERROR) << "not support optional vertex column as input currently";
        RETURN_UNSUPPORTED_ERROR(
            "not support optional vertex column as input currently");
      }
      auto& triplet = params.labels[0];
      auto pt = EdgeExpand::parse_edge_property_type(
          graph.schema(), triplet.src_label, triplet.dst_label,
          triplet.edge_label);
      auto collector =
          state.getEdgeCollector<SDSLEdgeColumn, Direction, LabelTriplet,
                                 PropertyType>(Direction::kOut, triplet, pt);
      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            if (label == triplet.src_label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              bool has_edge = false;
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                collector.push_back_opt(index, v, nbr, oe_iter.GetData());
                oe_iter.Next();
                has_edge = true;
              }
              if (!has_edge) {
                collector.push_back_null(index);
              }
            } else {
              collector.push_back_null(index);
            }
          });

      return bl::result<void>();
    } else if (params.dir == Direction::kIn) {
      if (ctx.is_optional(params.v_tag)) {
        LOG(ERROR) << "not support optional vertex column as input currently";
        RETURN_UNSUPPORTED_ERROR(
            "not support optional vertex column as input currently");
      }
      auto& triplet = params.labels[0];
      auto props = graph.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;
      }
      auto builder =
          state.getEdgeCollector<SDSLEdgeColumn, Direction, LabelTriplet,
                                 PropertyType>(Direction::kIn, triplet, pt);
      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            if (label == triplet.dst_label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              bool has_edge = false;
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                builder.push_back_opt(index, nbr, v, ie_iter.GetData());
                ie_iter.Next();
                has_edge = true;
              }
              if (!has_edge) {
                builder.push_back_null(index);
              }
            } else {
              builder.push_back_null(index);
            }
          });

      return bl::result<void>();
    }
  }
  LOG(ERROR) << "not support" << params.labels.size() << " "
             << (int) params.dir;
  RETURN_UNSUPPORTED_ERROR("not support" +
                           std::to_string(params.labels.size()) + " " +
                           std::to_string((int) params.dir));
}

bl::result<void> EdgeExpand::expand_edge_without_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, LocalEdgeExpandState& state) {
  if (params.is_optional) {
    return expand_edge_without_predicate_optional_impl(graph, std::move(ctx),
                                                       params, state);
  }
  std::vector<size_t> shuffle_offset;
  if (params.labels.size() == 1) {
    if (params.dir == Direction::kIn) {
      label_t output_vertex_label = params.labels[0].src_label;
      label_t edge_label = params.labels[0].edge_label;
      PropertyType pt = EdgeExpand::parse_edge_property_type(
          graph.schema(), params.labels[0].src_label,
          params.labels[0].dst_label, params.labels[0].edge_label);
      auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                              LabelTriplet, PropertyType>(
          Direction::kIn, params.labels[0], pt);

      label_t dst_label = params.labels[0].dst_label;
      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            if (label != dst_label) {
              return;
            }
            auto ie_iter = graph.GetInEdgeIterator(
                label, v, output_vertex_label, edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              assert(ie_iter.GetData().type == pt);
              collector.push_back_opt(index, nbr, v, ie_iter.GetData());

              ie_iter.Next();
            }
          });
      return bl::result<void>();
    } else if (params.dir == Direction::kOut) {
      label_t output_vertex_label = params.labels[0].dst_label;
      label_t edge_label = params.labels[0].edge_label;

      PropertyType pt = parse_edge_property_type(
          graph.schema(), params.labels[0].src_label,
          params.labels[0].dst_label, params.labels[0].edge_label);

      auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                              LabelTriplet, PropertyType>(
          Direction::kOut, params.labels[0], pt);
      label_t src_label = params.labels[0].src_label;
      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            if (label != src_label) {
              return;
            }
            auto oe_iter = graph.GetOutEdgeIterator(
                label, v, output_vertex_label, edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              assert(oe_iter.GetData().type == pt);
              collector.push_back_opt(index, v, nbr, oe_iter.GetData());
              oe_iter.Next();
            }
          });

      return bl::result<void>();

    } else {
      auto pt = EdgeExpand::parse_edge_property_type(
          graph.schema(), params.labels[0].src_label,
          params.labels[0].dst_label, params.labels[0].edge_label);
      auto collector =
          state.getEdgeCollector<BDSLEdgeColumn, LabelTriplet, PropertyType>(
              params.labels[0], pt);

      ctx.foreach_vertex(
          params.v_tag, [&](size_t index, label_t label, vid_t v) {
            if (label == params.labels[0].src_label) {
              auto oe_iter =
                  graph.GetOutEdgeIterator(label, v, params.labels[0].dst_label,
                                           params.labels[0].edge_label);
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                collector.push_back_opt(index, v, nbr, oe_iter.GetData(),
                                        Direction::kOut);
                oe_iter.Next();
              }
            }
            if (label == params.labels[0].dst_label) {
              auto ie_iter =
                  graph.GetInEdgeIterator(label, v, params.labels[0].src_label,
                                          params.labels[0].edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                collector.push_back_opt(index, nbr, v, ie_iter.GetData(),
                                        Direction::kIn);
                ie_iter.Next();
              }
            }
          });
      return bl::result<void>();
    }
  } else {
    auto label_set = ctx.get_vertex_labels_set(params.v_tag);
    auto labels =
        get_expand_label_set(graph, label_set, params.labels, params.dir);
    std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
    std::vector<std::vector<PropertyType>> props_vec;
    std::vector<std::vector<LabelTriplet>> in_labels_map(
        graph.schema().vertex_label_num()),
        out_labels_map(graph.schema().vertex_label_num());
    for (const auto& triplet : labels) {
      in_labels_map[triplet.dst_label].emplace_back(triplet);
      out_labels_map[triplet.src_label].emplace_back(triplet);
      PropertyType pt = EdgeExpand::parse_edge_property_type(
          graph.schema(), triplet.src_label, triplet.dst_label,
          triplet.edge_label);
      label_props.emplace_back(triplet, pt);
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kIn) {
      if (labels.size() == 1) {
        if (params.dir == Direction::kOut) {
          auto& triplet = labels[0];
          auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                                  LabelTriplet, PropertyType>(
              Direction::kOut, triplet, label_props[0].second);
          ctx.foreach_vertex(
              params.v_tag, [&](size_t index, label_t label, vid_t v) {
                if (label == triplet.src_label) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    collector.push_back_opt(index, v, nbr, oe_iter.GetData());
                    oe_iter.Next();
                  }
                }
              });
          return bl::result<void>();
        } else if (params.dir == Direction::kIn) {
          auto& triplet = labels[0];
          auto collector = state.getEdgeCollector<SDSLEdgeColumn, Direction,
                                                  LabelTriplet, PropertyType>(
              Direction::kIn, triplet, label_props[0].second);
          ctx.foreach_vertex(
              params.v_tag, [&](size_t index, label_t label, vid_t v) {
                if (label == triplet.dst_label) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    collector.push_back_opt(index, nbr, v, ie_iter.GetData());

                    ie_iter.Next();
                  }
                }
              });
          return bl::result<void>();
        }
      } else if (labels.size() > 1 || labels.size() == 0) {
        auto builder = state.getEdgeCollector<
            SDMLEdgeColumn, Direction,
            std::vector<std::pair<LabelTriplet, PropertyType>>>(params.dir,
                                                                label_props);

        if (params.dir == Direction::kOut) {
          ctx.foreach_vertex(
              params.v_tag, [&](size_t index, label_t label, vid_t v) {
                for (const auto& triplet : out_labels_map[label]) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(index, triplet, v, nbr,
                                          oe_iter.GetData());
                    oe_iter.Next();
                  }
                }
              });
        } else {
          ctx.foreach_vertex(
              params.v_tag, [&](size_t index, label_t label, vid_t v) {
                for (const auto& triplet : in_labels_map[label]) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(index, triplet, nbr, v,
                                          ie_iter.GetData());

                    ie_iter.Next();
                  }
                }
              });
        }

        return bl::result<void>();
      }
    } else if (params.dir == Direction::kBoth) {
      if (labels.size() == 1) {
        auto builder =
            state.getEdgeCollector<BDSLEdgeColumn, LabelTriplet, PropertyType>(
                labels[0], label_props[0].second);
        ctx.foreach_vertex(
            params.v_tag, [&](size_t index, label_t label, vid_t v) {
              if (label == labels[0].src_label) {
                auto oe_iter = graph.GetOutEdgeIterator(
                    label, v, labels[0].dst_label, labels[0].edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  builder.push_back_opt(index, v, nbr, oe_iter.GetData(),
                                        Direction::kOut);
                  oe_iter.Next();
                }
              }
              if (label == labels[0].dst_label) {
                auto ie_iter = graph.GetInEdgeIterator(
                    label, v, labels[0].src_label, labels[0].edge_label);
                while (ie_iter.IsValid()) {
                  auto nbr = ie_iter.GetNeighbor();
                  builder.push_back_opt(index, nbr, v, ie_iter.GetData(),
                                        Direction::kIn);
                  shuffle_offset.push_back(index);
                }
              }
            });
        return bl::result<void>();
      } else {
        auto builder = state.getEdgeCollector<
            BDMLEdgeColumn, std::vector<std::pair<LabelTriplet, PropertyType>>>(
            label_props);
        ctx.foreach_vertex(
            params.v_tag, [&](size_t index, label_t label, vid_t v) {
              for (const auto& triplet : out_labels_map[label]) {
                auto oe_iter = graph.GetOutEdgeIterator(
                    label, v, triplet.dst_label, triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  builder.push_back_opt(index, triplet, v, nbr,
                                        oe_iter.GetData(), Direction::kOut);
                  oe_iter.Next();
                }
              }
              for (const auto& triplet : in_labels_map[label]) {
                auto ie_iter = graph.GetInEdgeIterator(
                    label, v, triplet.src_label, triplet.edge_label);
                while (ie_iter.IsValid()) {
                  auto nbr = ie_iter.GetNeighbor();
                  builder.push_back_opt(index, triplet, nbr, v,
                                        ie_iter.GetData(), Direction::kIn);
                  ie_iter.Next();
                }
              }
            });
        return bl::result<void>();
      }
    }
  }

  LOG(ERROR) << "not support" << params.labels.size() << " "
             << (int) params.dir;
  RETURN_UNSUPPORTED_ERROR("not support" +
                           std::to_string(params.labels.size()) + " " +
                           std::to_string((int) params.dir));
}

bl::result<void> EdgeExpand::expand_vertex_without_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, LocalEdgeExpandState& state) {
  VertexColumnType input_vertex_list_type =
      ctx.get_vertex_column_type(params.v_tag);
  if (input_vertex_list_type == VertexColumnType::kSingle) {
    if (ctx.is_optional(params.v_tag)) {
      expand_vertex_without_predicate_optional_impl<SLVertexColumn>(
          graph, ctx, params.v_tag, params.labels, params.dir, state);
      return bl::result<void>();
    } else {
      // optional edge expand
      if (params.is_optional) {
        expand_vertex_without_predicate_optional_impl<SLVertexColumn>(
            graph, ctx, params.v_tag, params.labels, params.dir, state);
        return bl::result<void>();
      } else {
        expand_vertex_without_predicate_impl<SLVertexColumn>(
            graph, ctx, params.v_tag, params.labels, params.dir, state);
        return bl::result<void>();
      }
    }
  } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
    if (ctx.is_optional(params.v_tag) || params.is_optional) {
      expand_vertex_without_predicate_optional_impl<MLVertexColumn>(
          graph, ctx, params.v_tag, params.labels, params.dir, state);
      return bl::result<void>();
    }
    expand_vertex_without_predicate_impl<MLVertexColumn>(
        graph, ctx, params.v_tag, params.labels, params.dir, state);
    return bl::result<void>();
  } /*else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
    if (input_vertex_list->is_optional() || params.is_optional) {
      LOG(ERROR) << "not support optional vertex column as input currently";
      RETURN_UNSUPPORTED_ERROR(
          "not support optional vertex column as input currently");
    }
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
    auto pair = expand_vertex_without_predicate_impl(
        graph, *casted_input_vertex_list, params.labels, params.dir);
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

template <typename T>
bl::result<void> expand_vertex_ep_lt_ml_impl(
    const GraphReadInterface& graph, const DataChunk& ctx, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    label_t input_label, const std::string& ep_val, int alias,
    LocalEdgeExpandState& state) {
  T max_value(gs::runtime::TypedConverter<T>::typed_from_string(ep_val));
  std::vector<GraphReadInterface::graph_view_t<T>> views;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    if (dir == Direction::kOut) {
      views.emplace_back(
          graph.GetOutgoingGraphView<T>(input_label, nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(
          graph.GetIncomingGraphView<T>(input_label, nbr_label, edge_label));
    }
  }
  auto builder = state.getEdgeCollector<MLVertexColumn>();
  size_t csr_idx = 0;
  std::vector<size_t> offsets;
  for (auto& csr : views) {
    label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
    ctx.foreach_vertex(v_tag, [&](size_t idx, label_t label, vid_t v) {
      csr.foreach_edges_lt(v, max_value, [&](vid_t nbr, const T& e) {
        builder.push_back_opt(idx, nbr_label, nbr);
      });
    });
    ++csr_idx;
  }

  return bl::result<void>();
}
bl::result<void> EdgeExpand::expand_vertex_ep_lt(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, const std::string& ep_val,
    LocalEdgeExpandState& state) {
  if (params.is_optional) {
    LOG(ERROR) << "not support optional edge expand";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }

  VertexColumnType input_vertex_list_type =
      ctx.get_vertex_column_type(params.v_tag);

  if (input_vertex_list_type == VertexColumnType::kSingle) {
    label_t input_label = *ctx.get_vertex_labels_set(params.v_tag).begin();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    std::vector<PropertyType> ed_types;
    for (auto& triplet : params.labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((params.dir == Direction::kOut) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          if (properties.size() != 1) {
            LOG(ERROR) << "not support edge type";
            RETURN_UNSUPPORTED_ERROR("not support edge type");
          }
          ed_types.push_back(properties[0]);
        }
      }
      if (triplet.dst_label == input_label &&
          ((params.dir == Direction::kIn) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          if (properties.size() != 1) {
            LOG(ERROR) << "not support edge type";
            RETURN_UNSUPPORTED_ERROR("not support edge type");
          }
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
    if (!sp) {
      LOG(ERROR) << "not support edge type";
      RETURN_UNSUPPORTED_ERROR("not support edge type");
    }
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Date()) {
      return expand_vertex_ep_lt_ml_impl<Date>(graph, ctx, params.v_tag,
                                               label_dirs, input_label, ep_val,
                                               params.alias, state);
    } else if (ed_type == PropertyType::Int64()) {
      return expand_vertex_ep_lt_ml_impl<int64_t>(graph, ctx, params.v_tag,
                                                  label_dirs, input_label,
                                                  ep_val, params.alias, state);
    } else {
      LOG(ERROR) << "not support edge type";
      RETURN_UNSUPPORTED_ERROR("not support edge type");
    }
  } else {
    LOG(ERROR) << "not support vertex column type";
    RETURN_UNSUPPORTED_ERROR("not support vertex column type");
  }
}

template <typename T>
bl::result<void> expand_vertex_ep_gt_sl_impl(
    const GraphReadInterface& graph, const DataChunk& ctx, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    label_t input_label, const std::string& ep_val, int alias,
    LocalEdgeExpandState& state) {
  T max_value(gs::runtime::TypedConverter<T>::typed_from_string(ep_val));
  std::vector<GraphReadInterface::graph_view_t<T>> views;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    if (dir == Direction::kOut) {
      views.emplace_back(
          graph.GetOutgoingGraphView<T>(input_label, nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(
          graph.GetIncomingGraphView<T>(input_label, nbr_label, edge_label));
    }
  }
  auto builder =
      state.getEdgeCollector<SLVertexColumn>(std::get<0>(label_dirs[0]));
  std::vector<size_t> offsets;
  for (auto& csr : views) {
    ctx.foreach_vertex(v_tag, [&](size_t idx, label_t label, vid_t v) {
      csr.foreach_edges_gt(v, max_value, [&](vid_t nbr, const T& val) {
        builder.push_back_opt(idx, nbr);
        offsets.push_back(idx);
      });
    });
  }

  return bl::result<void>();
}

template <typename T>
bl::result<void> expand_vertex_ep_gt_ml_impl(
    const GraphReadInterface& graph, const DataChunk& ctx, int v_tag,
    const std::vector<std::tuple<label_t, label_t, Direction>>& label_dirs,
    label_t input_label, const EdgeExpandParams& params,
    const std::string& ep_val, int alias, LocalEdgeExpandState& state) {
  T max_value = gs::runtime::TypedConverter<T>::typed_from_string(ep_val);
  std::vector<GraphReadInterface::graph_view_t<T>> views;
  for (auto& t : label_dirs) {
    label_t nbr_label = std::get<0>(t);
    label_t edge_label = std::get<1>(t);
    Direction dir = std::get<2>(t);
    if (dir == Direction::kOut) {
      views.emplace_back(
          graph.GetOutgoingGraphView<T>(input_label, nbr_label, edge_label));
    } else {
      CHECK(dir == Direction::kIn);
      views.emplace_back(
          graph.GetIncomingGraphView<T>(input_label, nbr_label, edge_label));
    }
  }
  auto builder = state.getEdgeCollector<MLVertexColumn>();
  size_t csr_idx = 0;
  std::vector<size_t> offsets;
  for (auto& csr : views) {
    label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
    ctx.foreach_vertex(v_tag, [&](size_t idx, label_t label, vid_t v) {
      csr.foreach_edges_gt(v, max_value, [&](vid_t nbr, const T& val) {
        builder.push_back_opt(idx, nbr_label, nbr);
      });
    });
    ++csr_idx;
  }

  return bl::result<void>();
}

bl::result<void> EdgeExpand::expand_vertex_ep_gt(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, const std::string& ep_val,
    LocalEdgeExpandState& state) {
  if (params.is_optional) {
    LOG(ERROR) << "not support optional edge expand";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }

  VertexColumnType input_vertex_list_type =
      ctx.get_vertex_column_type(params.v_tag);

  if (input_vertex_list_type == VertexColumnType::kSingle) {
    label_t input_label = *ctx.get_vertex_labels_set(params.v_tag).begin();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    std::vector<PropertyType> ed_types;
    for (auto& triplet : params.labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((params.dir == Direction::kOut) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          if (properties.size() != 1) {
            LOG(ERROR) << "not support multiple edge types";
            RETURN_UNSUPPORTED_ERROR("not support multiple edge types");
          }
          ed_types.push_back(properties[0]);
        }
      }
      if (triplet.dst_label == input_label &&
          ((params.dir == Direction::kIn) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          if (properties.size() != 1) {
            LOG(ERROR) << "not support multiple edge types";
            RETURN_UNSUPPORTED_ERROR("not support multiple edge types");
          }
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
    if (!sp) {
      LOG(ERROR) << "not support multiple edge types";
      RETURN_UNSUPPORTED_ERROR("not support multiple edge types");
    }
    const PropertyType& ed_type = ed_types[0];
    if (se) {
      if (ed_type == PropertyType::Date()) {
        return expand_vertex_ep_gt_sl_impl<Date>(graph, ctx, params.v_tag,
                                                 label_dirs, input_label,
                                                 ep_val, params.alias, state);
      } else if (ed_type == PropertyType::Int64()) {
        return expand_vertex_ep_gt_sl_impl<int64_t>(
            graph, ctx, params.v_tag, label_dirs, input_label, ep_val,
            params.alias, state);
      } else {
        LOG(ERROR) << "not support edge type" << ed_type.ToString();
        RETURN_UNSUPPORTED_ERROR("not support edge type " + ed_type.ToString());
      }
    } else {
      if (ed_type == PropertyType::Date()) {
        return expand_vertex_ep_gt_ml_impl<Date>(
            graph, ctx, params.v_tag, label_dirs, input_label, params, ep_val,
            params.alias, state);
      } else if (ed_type == PropertyType::Int64()) {
        return expand_vertex_ep_gt_ml_impl<int64_t>(
            graph, ctx, params.v_tag, label_dirs, input_label, params, ep_val,
            params.alias, state);
      } else {
        LOG(ERROR) << "not support edge type" << ed_type.ToString();
        RETURN_UNSUPPORTED_ERROR("not support edge type " + ed_type.ToString());
      }
    }
  } else {
    LOG(ERROR) << "unexpected to reach here...";
    RETURN_UNSUPPORTED_ERROR("unexpected to reach here...");
  }
}

template <typename T>
static bl::result<void> _expand_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, const gs::runtime::SPVertexPredicate& pred,
    LocalEdgeExpandState& state) {
  if (pred.type() == gs::runtime::SPPredicateType::kPropertyEQ) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<gs::runtime::VertexPropertyEQPredicateBeta<T>>>(
        graph, ctx, params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const gs::runtime::VertexPropertyEQPredicateBeta<T>&>(
                pred)),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyLT) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<gs::runtime::VertexPropertyLTPredicateBeta<T>>>(
        graph, ctx, params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const gs::runtime::VertexPropertyLTPredicateBeta<T>&>(
                pred)),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyGT) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<gs::runtime::VertexPropertyGTPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const gs::runtime::VertexPropertyGTPredicateBeta<T>&>(
                pred)),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyLE) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<gs::runtime::VertexPropertyLEPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const gs::runtime::VertexPropertyLEPredicateBeta<T>&>(
                pred)),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyGE) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<gs::runtime::VertexPropertyGEPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const gs::runtime::VertexPropertyGEPredicateBeta<T>&>(
                pred)),
        state);
  } else if (pred.type() == gs::runtime::SPPredicateType::kPropertyBetween) {
    return EdgeExpand::expand_vertex<EdgeExpand::SPVPWrapper<
        gs::runtime::VertexPropertyBetweenPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<
                const gs::runtime::VertexPropertyBetweenPredicateBeta<T>&>(
                pred)),
        state);
  } else {
    LOG(ERROR) << "not support vertex property type "
               << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR("not support vertex property type " +
                             std::to_string(static_cast<int>(pred.type())));
  }
}

bl::result<void> EdgeExpand::expand_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const EdgeExpandParams& params, const gs::runtime::SPVertexPredicate& pred,
    LocalEdgeExpandState& state) {
  if (params.is_optional) {
    LOG(ERROR) << "not support optional edge expand";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }

  if (pred.data_type() == RTAnyType::kI64Value) {
    return _expand_vertex_with_special_vertex_predicate<int64_t>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _expand_vertex_with_special_vertex_predicate<Date>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_vertex_with_special_vertex_predicate<double>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _expand_vertex_with_special_vertex_predicate<std::string_view>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _expand_vertex_with_special_vertex_predicate<int32_t>(
        graph, ctx, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kDate32) {
    return _expand_vertex_with_special_vertex_predicate<Day>(graph, ctx, params,
                                                             pred, state);
  }
  LOG(ERROR) << "not support vertex property type "
             << static_cast<int>(pred.data_type());
  RETURN_UNSUPPORTED_ERROR("not support vertex property type " +
                           std::to_string(static_cast<int>(pred.data_type())));
}
}  // namespace ops

}  // namespace chunked_runtime
}  // namespace gs