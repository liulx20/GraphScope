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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/get_v_state.h"
#include "flex/engines/graph_db/runtime/utils/params.h"
namespace gs {
namespace chunked_runtime {
namespace ops {

using gs::runtime::GetVParams;
using gs::runtime::LabelTriplet;
using gs::runtime::VOpt;
inline std::vector<label_t> extract_labels(
    const std::vector<LabelTriplet>& labels, const std::vector<label_t>& tables,
    VOpt opt) {
  std::vector<label_t> output_labels;
  for (const auto& label : labels) {
    if (opt == VOpt::kStart) {
      if (std::find(tables.begin(), tables.end(), label.src_label) !=
              tables.end() ||
          tables.empty()) {
        output_labels.push_back(label.src_label);
      }
    } else if (opt == VOpt::kEnd) {
      if (std::find(tables.begin(), tables.end(), label.dst_label) !=
              tables.end() ||
          tables.empty()) {
        output_labels.push_back(label.dst_label);
      }
    } else {
      LOG(ERROR) << "not support" << static_cast<int>(opt);
    }
  }
  return output_labels;
}

class GetV {
 public:
  template <typename PRED_T>
  static bl::result<void> get_vertex_from_edges_optional_impl(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const GetVParams& params, const PRED_T& pred, LocalGetVState& state) {
    auto edge_col_type = ctx.get_edge_column_type(params.tag);
    if (edge_col_type == EdgeColumnType::kBDSL) {
      auto builder = state.getVertexCollector<SLVertexColumn, label_t>(
          ctx.get_edge_labels(params.tag)[0].src_label);

      ctx.foreach_edge(params.tag,
                       [&](size_t index, const LabelTriplet& label, vid_t src,
                           vid_t dst, const EdgeData& edata, Direction dir) {
                         if (src == std::numeric_limits<vid_t>::max() &&
                             dst == std::numeric_limits<vid_t>::max()) {
                           if (pred(label.src_label, src, 0)) {
                             builder.push_back_null(index);
                           }
                         } else {
                           if (dir == Direction::kOut) {
                             if (label.dst_label == params.tables[0]) {
                               if (pred(label.dst_label, dst, index)) {
                                 builder.push_back(index, dst);
                               }
                             }
                           } else {
                             if (label.src_label == params.tables[0]) {
                               if (pred(label.src_label, src, index)) {
                                 builder.push_back(index, src);
                               }
                             }
                           }
                         }
                       });

      return bl::result<void>();
    } else if (edge_col_type == EdgeColumnType::kSDSL) {
      label_t output_vertex_label{0};
      if (params.opt == VOpt::kEnd) {
        output_vertex_label = ctx.get_edge_labels(params.tag)[0].dst_label;
      } else {
        output_vertex_label = ctx.get_edge_labels(params.tag)[0].src_label;
      }
      auto builder = state.getVertexCollector<SLVertexColumn, label_t>(
          output_vertex_label);
      if (params.opt == VOpt::kEnd) {
        ctx.foreach_edge(params.tag,
                         [&](size_t index, const LabelTriplet& label, vid_t src,
                             vid_t dst, const EdgeData& edata, Direction dir) {
                           if (src == std::numeric_limits<vid_t>::max() &&
                               dst == std::numeric_limits<vid_t>::max()) {
                             if (pred(label.src_label, src, 0)) {
                               builder.push_back_null(index);
                             }
                           } else {
                             if (label.dst_label == params.tables[0]) {
                               if (pred(label.dst_label, dst)) {
                                 builder.push_back(index, dst);
                               }
                             }
                           }
                         });
      }
      return bl::result<void>();
    }
    LOG(ERROR) << "Unsupported edge column type: "
               << static_cast<int>(edge_col_type);
    RETURN_UNSUPPORTED_ERROR("Unsupported edge column type: " +
                             std::to_string(static_cast<int>(edge_col_type)));
  }
  template <typename PRED_T>
  static bl::result<void> get_vertex_from_edges(const GraphReadInterface& graph,
                                                const DataChunk& ctx,
                                                const GetVParams& params,
                                                const PRED_T& pred,
                                                LocalGetVState& state) {
    auto column_type = ctx.get_column_type(params.tag);
    if (column_type == ContextColumnType::kPath) {
      auto builder = state.getVertexCollector<MLVertexColumn>();
      ctx.foreach_path(params.tag,
                       [&](size_t index, const gs::runtime::Path& path) {
                         auto [label, vid] = path.get_end();
                         builder.push_back(index, label, vid);
                       });

      return bl::result<void>();
    }

    if (ctx.is_optional(params.tag)) {
      return get_vertex_from_edges_optional_impl(graph, ctx, params, pred,
                                                 state);
    }

    auto edge_col_type = ctx.get_edge_column_type(params.tag);

    if (edge_col_type == EdgeColumnType::kSDSL) {
      label_t output_vertex_label{0};
      auto edge_label = ctx.get_edge_labels(params.tag)[0];

      VOpt opt = params.opt;
      if (params.opt == VOpt::kOther) {
        if (ctx.get_edge_direction(params.tag) == Direction::kOut) {
          opt = VOpt::kEnd;
        } else {
          opt = VOpt::kStart;
        }
      }
      if (opt == VOpt::kStart) {
        output_vertex_label = edge_label.src_label;
      } else if (opt == VOpt::kEnd) {
        output_vertex_label = edge_label.dst_label;
      } else {
        LOG(ERROR) << "not support GetV opt " << static_cast<int>(opt);
        RETURN_UNSUPPORTED_ERROR("not support GetV opt " +
                                 std::to_string(static_cast<int>(opt)));
      }
      // params tables size may be 0
      if (params.tables.size() == 1) {
        if (output_vertex_label != params.tables[0]) {
          LOG(ERROR) << "output_vertex_label != params.tables[0]"
                     << static_cast<int>(output_vertex_label) << " "
                     << static_cast<int>(params.tables[0]);
          RETURN_BAD_REQUEST_ERROR("output_vertex_label != params.tables[0]");
        }
      }
      auto builder = state.getVertexCollector<SLVertexColumn, label_t>(
          output_vertex_label);
      if (opt == VOpt::kStart) {
        ctx.foreach_edge(params.tag,
                         [&](size_t index, const LabelTriplet& label, vid_t src,
                             vid_t dst, const EdgeData& edata, Direction dir) {
                           if (pred(label.src_label, src, index)) {
                             builder.push_back(index, src);
                           }
                         });
      } else if (opt == VOpt::kEnd) {
        ctx.foreach_edge(params.tag,
                         [&](size_t index, const LabelTriplet& label, vid_t src,
                             vid_t dst, const EdgeData& edata, Direction dir) {
                           if (pred(label.dst_label, dst, index)) {
                             builder.push_back(index, dst);
                           }
                         });
      }
      return bl::result<void>();
    } else if (edge_col_type == EdgeColumnType::kSDML) {
      VOpt opt = params.opt;
      if (params.opt == VOpt::kOther) {
        if (ctx.get_edge_direction(params.tag) == Direction::kOut) {
          opt = VOpt::kEnd;
        } else {
          opt = VOpt::kStart;
        }
      }

      auto labels =
          extract_labels(ctx.get_edge_labels(params.tag), params.tables, opt);
      if (labels.size() == 0) {
        // auto builder = MLVertexColumnBuilder::builder();
        // ctx.set_with_reshuffle(params.alias, builder.finish(nullptr), {});
        return bl::result<void>();
      }
      if (labels.size() > 1) {
        auto builder = state.getVertexCollector<MLVertexColumn>();
        if (opt == VOpt::kStart) {
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (std::find(labels.begin(), labels.end(), label.src_label) !=
                    labels.end()) {
                  builder.push_back(index, label.src_label, src);
                }
              });
        } else if (opt == VOpt::kEnd) {
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (std::find(labels.begin(), labels.end(), label.dst_label) !=
                    labels.end()) {
                  builder.push_back(index, label.dst_label, dst);
                }
              });
        }
        return bl::result<void>();
      }
    } else if (edge_col_type == EdgeColumnType::kBDSL) {
      if (params.tables.size() == 0) {
        auto type = ctx.get_edge_labels(params.tag)[0];
        if (type.src_label != type.dst_label) {
          auto builder = state.getVertexCollector<MLVertexColumn>();
          if (params.opt != VOpt::kOther) {
            LOG(ERROR) << "not support GetV opt "
                       << static_cast<int>(params.opt);
            RETURN_UNSUPPORTED_ERROR(
                "not support GetV opt " +
                std::to_string(static_cast<int>(params.opt)));
          }
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  builder.push_back(index, label.dst_label, dst);
                } else {
                  builder.push_back(index, label.src_label, src);
                }
              });
          return bl::result<void>();
        } else {
          auto builder =
              state.getVertexCollector<SLVertexColumn, label_t>(type.src_label);
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  builder.push_back(index, dst);
                } else {
                  builder.push_back(index, src);
                }
              });
          return bl::result<void>();
        }
      } else {
        std::vector<label_t> labels;
        auto type = ctx.get_edge_labels(params.tag)[0];
        for (auto& label : params.tables) {
          if (label == type.src_label || label == type.dst_label) {
            labels.push_back(label);
          }
        }
        if (labels.size() == 1) {
          auto builder =
              state.getVertexCollector<SLVertexColumn, label_t>(labels[0]);
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (label.dst_label == labels[0]) {
                    builder.push_back(index, dst);
                  }
                } else {
                  if (label.src_label == labels[0]) {
                    builder.push_back(index, src);
                  }
                }
              });
          return bl::result<void>();
        } else {
          auto builder = state.getVertexCollector<MLVertexColumn>();
          ctx.foreach_edge(params.tag, [&](size_t index,
                                           const LabelTriplet& label, vid_t src,
                                           vid_t dst, const EdgeData& edata,
                                           Direction dir) {
            if (dir == Direction::kOut) {
              if (std::find(labels.begin(), labels.end(), label.dst_label) !=
                  labels.end()) {
                builder.push_back(index, label.dst_label, dst);
              }
            } else {
              if (std::find(labels.begin(), labels.end(), label.src_label) !=
                  labels.end()) {
                builder.push_back(index, label.src_label, src);
              }
            }
          });
          return bl::result<void>();
        }
      }
    } else if (edge_col_type == EdgeColumnType::kBDML) {
      if (params.tables.size() == 0) {
        auto builder = state.getVertexCollector<MLVertexColumn>();
        if (params.opt != VOpt::kOther) {
          LOG(ERROR) << "not support GetV opt " << static_cast<int>(params.opt);
          RETURN_UNSUPPORTED_ERROR(
              "not support GetV opt " +
              std::to_string(static_cast<int>(params.opt)));
        }
        ctx.foreach_edge(params.tag,
                         [&](size_t index, const LabelTriplet& label, vid_t src,
                             vid_t dst, const EdgeData& edata, Direction dir) {
                           if (dir == Direction::kOut) {
                             builder.push_back(index, label.dst_label, dst);
                           } else {
                             builder.push_back(index, label.src_label, src);
                           }
                         });
        return bl::result<void>();
      } else {
        if (params.tables.size() == 1) {
          auto vlabel = params.tables[0];
          auto builder =
              state.getVertexCollector<SLVertexColumn, label_t>(vlabel);
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (label.dst_label == vlabel) {
                    builder.push_back(index, dst);
                  }
                } else {
                  if (label.src_label == vlabel) {
                    builder.push_back(index, src);
                  }
                }
              });
          return bl::result<void>();
        } else {
          std::vector<bool> labels(graph.schema().vertex_label_num(), false);
          for (auto& label : params.tables) {
            labels[label] = true;
          }
          auto builder = state.getVertexCollector<MLVertexColumn>();
          ctx.foreach_edge(
              params.tag,
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (labels[label.dst_label]) {
                    builder.push_back(index, label.dst_label, dst);
                  }
                } else {
                  if (labels[label.src_label]) {
                    builder.push_back(index, label.src_label, src);
                  }
                }
              });
          return bl::result<void>();
        }
      }
    }

    LOG(ERROR) << "Unsupported edge column type: "
               << static_cast<int>(edge_col_type);
    RETURN_UNSUPPORTED_ERROR("Unsupported edge column type: " +
                             std::to_string(static_cast<int>(edge_col_type)));
  }
  template <typename PRED_T>
  static bl::result<void> get_vertex_from_vertices(
      const GraphReadInterface& graph, const DataChunk& ctx,
      const GetVParams& params, const PRED_T& pred, LocalGetVState& state) {
    if (params.tag == params.alias) {
      auto builder = state.getOffsetCollector();
      ctx.foreach_vertex(params.tag, [&](size_t idx, label_t label, vid_t v) {
        if (pred(label, v)) {
          builder.push_back(idx);
        }
      });
    } else {
      const auto& label_set = ctx.get_vertex_labels_set(params.tag);
      if (label_set.size() == 1) {
        auto builder = state.getVertexCollector<SLVertexColumn, label_t>(
            *label_set.begin());
        ctx.foreach_vertex(params.tag, [&](size_t idx, label_t label, vid_t v) {
          if (pred(label, v)) {
            builder.push_back(idx, v);
          }
        });
      } else {
        auto builder = state.getVertexCollector<MLVertexColumn>();
        ctx.foreach_vertex(params.tag, [&](size_t idx, label_t label, vid_t v) {
          if (pred(label, v)) {
            builder.push_back(idx, label, v);
          }
        });
      }
    }
    return bl::result<void>();
  }
};

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_H_