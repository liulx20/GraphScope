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

#ifndef CHUNKED_RUNTIME_EXECUTE_OPS_RETRIEVE_PATH_EXPAND_H_
#define CHUNKED_RUNTIME_EXECUTE_OPS_RETRIEVE_PATH_EXPAND_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/edge_expand_state.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/path_expand_impl.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/path_expand_state.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::Direction;
using gs::runtime::LabelTriplet;
struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

struct ShortestPathParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  int v_alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

class PathExpand {
 public:
  static bl::result<void> edge_expand_v(
      const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
      const PathExpandParams& params, LocalEdgeExpandState& state);
  static bl::result<void> edge_expand_p(
      const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
      const PathExpandParams& params, LocalEdgeExpandState& state);

  static bl::result<void> all_shortest_paths_with_given_source_and_dest(
      const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
      const ShortestPathParams& params, const std::pair<label_t, vid_t>& dst,
      LocalPathState& state);
  // single dst
  static bl::result<void> single_source_single_dest_shortest_path(
      const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
      const ShortestPathParams& params, std::pair<label_t, vid_t>& dest,
      LocalPathState& state);

  template <typename PRED_T>
  static bl::result<void>
  single_source_shortest_path_with_order_by_length_limit(
      const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
      const ShortestPathParams& params, const PRED_T& pred, int limit_upper,
      LocalSSSPState& state) {
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        ctx.get_vertex_labels_set(params.start_tag).size() == 1) {
      const auto& properties = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        return single_source_shortest_path_with_order_by_length_limit_impl<
            grape::EmptyType, PRED_T>(graph, ctx, params.start_tag,
                                      params.labels[0].edge_label, params.dir,
                                      params.hop_lower, params.hop_upper, pred,
                                      limit_upper, state);
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          return single_source_shortest_path_with_order_by_length_limit_impl<
              int, PRED_T>(graph, ctx, params.start_tag,
                           params.labels[0].edge_label, params.dir,
                           params.hop_lower, params.hop_upper, pred,
                           limit_upper, state);
        } else if (properties[0] == PropertyType::Int64()) {
          return single_source_shortest_path_with_order_by_length_limit_impl<
              int64_t, PRED_T>(graph, ctx, params.start_tag,
                               params.labels[0].edge_label, params.dir,
                               params.hop_lower, params.hop_upper, pred,
                               limit_upper, state);

        } else if (properties[0] == PropertyType::Date()) {
          return single_source_shortest_path_with_order_by_length_limit_impl<
              Date, PRED_T>(graph, ctx, params.start_tag,
                            params.labels[0].edge_label, params.dir,
                            params.hop_lower, params.hop_upper, pred,
                            limit_upper, state);

        } else if (properties[0] == PropertyType::StringView()) {
          return single_source_shortest_path_with_order_by_length_limit_impl<
              std::string_view, PRED_T>(graph, ctx, params.start_tag,
                                        params.labels[0].edge_label, params.dir,
                                        params.hop_lower, params.hop_upper,
                                        pred, limit_upper, state);

        } else if (properties[0] == PropertyType::Double()) {
          return single_source_shortest_path_with_order_by_length_limit_impl<
              double, PRED_T>(graph, ctx, params.start_tag,
                              params.labels[0].edge_label, params.dir,
                              params.hop_lower, params.hop_upper, pred,
                              limit_upper, state);
        }
      }
    }

    LOG(ERROR) << "not support edge property type ";
    RETURN_UNSUPPORTED_ERROR("not support edge property type ");
  }
  /**
    template <typename PRED_T>
    static bl::result<void> single_source_shortest_path(
        const gs::runtime::GraphReadInterface& graph, const DataChunk& ctx,
        const ShortestPathParams& params, const PRED_T& pred) {
      std::vector<size_t> shuffle_offset;
      if (params.labels.size() == 1 &&
          params.labels[0].src_label == params.labels[0].dst_label &&
          params.dir == Direction::kBoth &&
          ctx.get_vertex_labels_set(params.start_tag).size() == 1) {
        const auto& properties = graph.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        if (properties.empty()) {
          auto tup = single_source_shortest_path_impl<grape::EmptyType, PRED_T>(
              graph, ctx, params.start_tag, params.labels[0].edge_label,
              params.dir, params.hop_lower, params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties.size() == 1) {
          if (properties[0] == PropertyType::Int32()) {
            auto tup = single_source_shortest_path_impl<int, PRED_T>(
                graph, ctx, params.start_tag, params.labels[0].edge_label,
                params.dir, params.hop_lower, params.hop_upper, pred);
            ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                   std::get<2>(tup));
            ctx.set(params.alias, std::get<1>(tup));
            return ctx;
          } else if (properties[0] == PropertyType::Int64()) {
            auto tup = single_source_shortest_path_impl<int64_t, PRED_T>(
                graph, ctx, params.start_tag, params.labels[0].edge_label,
                params.dir, params.hop_lower, params.hop_upper, pred);
            ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                   std::get<2>(tup));
            ctx.set(params.alias, std::get<1>(tup));
            return ctx;
          } else if (properties[0] == PropertyType::Date()) {
            auto tup = single_source_shortest_path_impl<Date, PRED_T>(
                graph, ctx, parmas.start_tag, params.labels[0].edge_label,
                params.dir, params.hop_lower, params.hop_upper, pred);
            ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                   std::get<2>(tup));
            ctx.set(params.alias, std::get<1>(tup));
            return ctx;
          } else if (properties[0] == PropertyType::Double()) {
            auto tup = single_source_shortest_path_impl<double, PRED_T>(
                graph, ctx, params.start_tag, params.labels[0].edge_label,
                params.dir, params.hop_lower, params.hop_upper, pred);
            ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                   std::get<2>(tup));
            ctx.set(params.alias, std::get<1>(tup));
            return ctx;
          }
        }
      }
      auto tup = default_single_source_shortest_path_impl<PRED_T>(
          graph, ctx, params.start_tag, params.labels, params.dir,
          params.hop_lower, params.hop_upper, pred);
      ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
    std::get<2>(tup)); ctx.set(params.alias, std::get<1>(tup)); return ctx;
    }*/
};

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif