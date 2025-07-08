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

#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/converter.h"
#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
void Converter::build_empty_context(gs::runtime::Context& ctx) {
  for (auto& [k, v] : ctx_meta_.metas()) {
    const auto& [type, elem_type] = v;
    if (type == gs::runtime::ContextColumnType::kVertex) {
      auto builder = gs::runtime::MLVertexColumnBuilder::builder();
      ctx.set(k, builder.finish(nullptr));
    } else if (type == gs::runtime::ContextColumnType::kEdge) {
      auto builder = gs::runtime::BDMLEdgeColumnBuilder::builder();
      ctx.set(k, builder.finish(nullptr));
    } else if (type == gs::runtime::ContextColumnType::kPath) {
      auto builder = gs::runtime::GeneralPathColumnBuilder();
      ctx.set(k, builder.finish(nullptr));
    } else {
      if (elem_type == gs::runtime::RTAnyType::kI32Value) {
        auto builder = gs::runtime::ValueColumnBuilder<int32_t>();
        ctx.set(k, builder.finish(nullptr));
      } else if (elem_type == gs::runtime::RTAnyType::kI64Value) {
        auto builder = gs::runtime::ValueColumnBuilder<int64_t>();
        ctx.set(k, builder.finish(nullptr));
      }
    }
  }
}

// for optional value columns
template <typename T>
void copy_column_data(
    gs::runtime::OptionalValueColumnBuilder<T>& builder,
    const ValueColumn<T>& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        T val = column.data_[i];
        if (column.valid_ == nullptr) {
          // If valid is not provided, assume all values are valid
          builder.push_back_opt(val, true);
        } else {
          // Check if the value is valid
          bool valid = (column.valid_[i / 8] & (1 << (i % 8)));
          builder.push_back_opt(val, valid);
        }
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        T val = column.data_[i];
        bool valid = true;
        if (column.valid_ != nullptr) {
          valid = (column.valid_[i / 8] & (1 << (i % 8)));
        }
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val, valid);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          T val = column.data_[idx];
          if (column.valid_ == nullptr) {
            // If valid is not provided, assume all values are valid
            builder.push_back_opt(val, true);
          } else {
            // Check if the value is valid
            bool valid = (column.valid_[idx / 8] & (1 << (idx % 8)));
            builder.push_back_opt(val, valid);
          }
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            T val = column.data_[idx];
            if (column.valid_ == nullptr) {
              // If valid is not provided, assume all values are valid
              builder.push_back_opt(val, true);
            } else {
              // Check if the value is valid
              bool valid = (column.valid_[idx / 8] & (1 << (idx % 8)));
              builder.push_back_opt(val, valid);
            }
          }
        }
      }
    }
  }
}

template <typename T>
void copy_column_data(
    gs::runtime::ValueColumnBuilder<T>& builder, const ValueColumn<T>& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        T val = column.data_[i];
        builder.push_back_opt(val);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        T val = column.data_[i];
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          T val = column.data_[idx];
          builder.push_back_opt(val);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            T val = column.data_[idx];
            builder.push_back_opt(val);
          }
        }
      }
    }
  }
}

void copy_sdsl_edge_column_data(
    gs::runtime::SDSLEdgeColumnBuilder& builder, const SDSLEdgeColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto [k, v] = column.edges_[i];
        auto prop = column.prop_col_.get(i);
        builder.push_back_opt(k, v, prop);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto [k, v] = column.edges_[i];
        auto prop = column.prop_col_.get(i);
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(k, v, prop);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          auto [k, v] = column.edges_[idx];
          auto prop = column.prop_col_.get(idx);
          builder.push_back_opt(k, v, prop);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            auto [val1, val2] = column.edges_[idx];
            auto val3 = column.prop_col_.get(idx);
            builder.push_back_opt(val1, val2, val3);
          }
        }
      }
    }
  }
}

void copy_sdml_edge_column_data(
    gs::runtime::SDMLEdgeColumnBuilder& builder, const SDMLEdgeColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& e = column.edges_[i];
        auto index = std::get<0>(e);
        auto label = column.edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto offset = std::get<3>(e);

        auto prop = column.prop_cols_[index].get(offset);
        builder.push_back_opt(label, src, dst, prop);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& e = column.edges_[i];
        auto index = std::get<0>(e);
        auto label = column.edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto off = std::get<3>(e);
        auto prop = column.prop_cols_[index].get(off);

        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(label, src, dst, prop);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;

          const auto& e = column.edges_[idx];
          auto index = std::get<0>(e);
          auto label = column.edge_labels_[index].first;
          auto src = std::get<1>(e);
          auto dst = std::get<2>(e);
          auto offs = std::get<3>(e);
          auto prop = column.prop_cols_[index].get(offs);
          builder.push_back_opt(label, src, dst, prop);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;

            const auto& e = column.edges_[idx];
            auto index = std::get<0>(e);
            auto label = column.edge_labels_[index].first;
            auto src = std::get<1>(e);
            auto dst = std::get<2>(e);
            auto offs = std::get<3>(e);
            auto prop = column.prop_cols_[index].get(offs);
            builder.push_back_opt(label, src, dst, prop);
          }
        }
      }
    }
  }
}

void copy_bdsl_edge_column_data(
    gs::runtime::BDSLEdgeColumnBuilder& builder, const BDSLEdgeColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& [src, dst, dir] = column.edges_[i];
        auto prop = column.prop_col_.get(i);

        builder.push_back_opt(
            src, dst, prop,
            dir ? gs::runtime::Direction::kOut : gs::runtime::Direction::kIn);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& [src, dst, dir] = column.edges_[i];
        auto prop = column.prop_col_.get(i);

        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(
              src, dst, prop,
              dir ? gs::runtime::Direction::kOut : gs::runtime::Direction::kIn);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;

          const auto& [src, dst, dir] = column.edges_[idx];
          auto prop = column.prop_col_.get(idx);

          builder.push_back_opt(
              src, dst, prop,
              dir ? gs::runtime::Direction::kOut : gs::runtime::Direction::kIn);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;

            const auto& [src, dst, dir] = column.edges_[idx];
            auto prop = column.prop_col_.get(idx);
            builder.push_back_opt(src, dst, prop,
                                  dir ? gs::runtime::Direction::kOut
                                      : gs::runtime::Direction::kIn);
          }
        }
      }
    }
  }
}

void copy_bdml_edge_column_data(
    gs::runtime::BDMLEdgeColumnBuilder& builder, const BDMLEdgeColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& e = column.edges_[i];
        auto index = std::get<0>(e);
        auto label = column.edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto offset = std::get<3>(e);
        auto dir = std::get<4>(e) ? gs::runtime::Direction::kOut
                                  : gs::runtime::Direction::kIn;

        auto prop = column.prop_cols_[index].get(offset);
        builder.push_back_opt(label, src, dst, prop, dir);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        const auto& e = column.edges_[i];
        auto index = std::get<0>(e);
        auto label = column.edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto off = std::get<3>(e);
        auto prop = column.prop_cols_[index].get(off);
        auto dir = std::get<4>(e) ? gs::runtime::Direction::kOut
                                  : gs::runtime::Direction::kIn;

        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(label, src, dst, prop, dir);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;

          const auto& e = column.edges_[idx];
          auto index = std::get<0>(e);
          auto label = column.edge_labels_[index].first;
          auto src = std::get<1>(e);
          auto dst = std::get<2>(e);
          auto offs = std::get<3>(e);
          auto prop = column.prop_cols_[index].get(offs);
          auto dir = std::get<4>(e) ? gs::runtime::Direction::kOut
                                    : gs::runtime::Direction::kIn;
          builder.push_back_opt(label, src, dst, prop, dir);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;

            const auto& e = column.edges_[idx];
            auto index = std::get<0>(e);
            auto label = column.edge_labels_[index].first;
            auto src = std::get<1>(e);
            auto dst = std::get<2>(e);
            auto offs = std::get<3>(e);
            auto prop = column.prop_cols_[index].get(offs);
            auto dir = std::get<4>(e) ? gs::runtime::Direction::kOut
                                      : gs::runtime::Direction::kIn;
            builder.push_back_opt(label, src, dst, prop, dir);
          }
        }
      }
    }
  }
}

void copy_sl_vertex_column_data(
    gs::runtime::SLVertexColumnBuilder& builder, const SLVertexColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        vid_t val = column.data_[i];
        builder.push_back_opt(val);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        vid_t val = column.data_[i];
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          vid_t val = column.data_[idx];
          builder.push_back_opt(val);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            vid_t val = column.data_[idx];
            builder.push_back_opt(val);
          }
        }
      }
    }
  }
}

void copy_sl_vertex_column_data(
    gs::runtime::SLVertexColumnBuilder& builder, const MLVertexColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        vid_t val = column.data_[i].vid_;
        builder.push_back_opt(val);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        vid_t val = column.data_[i].vid_;
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          vid_t val = column.data_[idx].vid_;
          builder.push_back_opt(val);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            vid_t val = column.data_[idx].vid_;
            builder.push_back_opt(val);
          }
        }
      }
    }
  }
}

void copy_ml_vertex_column_data(
    gs::runtime::MLVertexColumnBuilder& builder, const MLVertexColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        builder.push_back_opt(val);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;

        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          auto val = column.data_[idx];
          builder.push_back_opt(val);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            auto val = column.data_[idx];
            builder.push_back_opt(val);
          }
        }
      }
    }
  }
}

void copy_optional_path_column_data(
    gs::runtime::OptionalGeneralPathColumnBuilder& builder,
    const GeneralPathColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        bool valid = true;
        if (column.valid_ != nullptr) {
          valid = (column.valid_[i / 8] & (1 << (i % 8)));
        }
        builder.push_back_opt(val, valid);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        bool valid = true;
        if (column.valid_ != nullptr) {
          valid = (column.valid_[i / 8] & (1 << (i % 8)));
        }
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= ((*offset)[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val, valid);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          auto val = column.data_[idx];
          bool valid = true;
          if (column.valid_ != nullptr) {
            valid = (column.valid_[idx / 8] & (1 << (idx % 8)));
          }
          builder.push_back_opt(val, valid);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            auto val = column.data_[idx];
            bool valid = true;
            if (column.valid_ != nullptr) {
              valid = (column.valid_[idx / 8] & (1 << (idx % 8)));
            }
            builder.push_back_opt(val, valid);
          }
        }
      }
    }
  }
}

void copy_path_column_data(
    gs::runtime::GeneralPathColumnBuilder& builder,
    const GeneralPathColumn& column,
    const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets,
    int table_id) {
  if (table_id == 0) {
    if (offsets.size() == 0) {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        builder.push_back_opt(val);
      }
    } else {
      for (uint32_t i = 0; i < column.size(); ++i) {
        auto val = column.data_[i];
        size_t len = 1;
        for (const auto& offset : offsets) {
          len *= (((*offset)[i]) & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < len; ++j) {
          builder.push_back_opt(val);
        }
      }
    }
  } else {
    const auto& offset = *offsets[table_id - 1];
    if (offsets.size() == 1) {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        for (uint32_t j = 0; j < len; ++j) {
          uint32_t idx = off + j;
          auto val = column.data_[idx];
          builder.push_back_opt(val);
        }
      }
    } else {
      for (uint32_t i = 0; i < offset.size(); ++i) {
        uint32_t len = offset[i] & 0xFFFFFFFF;
        uint32_t off = offset[i] >> 32;
        size_t multiple = 1;
        for (size_t j = 0; j < offsets.size(); ++j) {
          if (j == static_cast<size_t>(table_id - 1))
            continue;
          multiple *= (offsets[j]->data_[i] & 0xFFFFFFFF);
        }
        for (size_t j = 0; j < multiple; ++j) {
          for (uint32_t k = 0; k < len; ++k) {
            uint32_t idx = off + k;
            auto val = column.data_[idx];
            builder.push_back_opt(val);
          }
        }
      }
    }
  }
}

std::shared_ptr<gs::runtime::IContextColumn> create_value_column(
    const std::vector<DataChunk>& chunks, int k, RTAnyType elem_type) {
  bool is_optional = false;
  for (const auto& chunk : chunks) {
    auto column = chunk.get(k);
    if (column->is_optional()) {
      is_optional = true;
      break;
    }
  }
  if (is_optional) {
    if (elem_type == RTAnyType::kI64Value) {
      gs::runtime::OptionalValueColumnBuilder<int64_t> builder;
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const ValueColumn<int64_t>*>(column);
        copy_column_data<int64_t>(builder, *casted_col, chunk.offsets(),
                                  TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else if (elem_type == RTAnyType::kI32Value) {
      gs::runtime::OptionalValueColumnBuilder<int32_t> builder;

      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const ValueColumn<int32_t>*>(column);
        copy_column_data<int32_t>(builder, *casted_col, chunk.offsets(),
                                  TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else {
      LOG(FATAL) << "Unsupported type for optional value column: "
                 << static_cast<int>(elem_type);
    }
  } else {
    if (elem_type == RTAnyType::kI64Value) {
      gs::runtime::ValueColumnBuilder<int64_t> builder;
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const ValueColumn<int64_t>*>(column);
        copy_column_data<int64_t>(builder, *casted_col, chunk.offsets(),
                                  TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);

    } else if (elem_type == RTAnyType::kI32Value) {
      gs::runtime::ValueColumnBuilder<int32_t> builder;
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const ValueColumn<int32_t>*>(column);
        copy_column_data<int32_t>(builder, *casted_col, chunk.offsets(),
                                  TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);

    } else {
      LOG(FATAL) << "Unsupported type for value column: "
                 << static_cast<int>(elem_type);
    }
  }
  return nullptr;
}

std::shared_ptr<gs::runtime::IContextColumn> create_path_column(
    const std::vector<DataChunk>& chunks, int k) {
  bool is_optional = false;
  for (const auto& chunk : chunks) {
    auto column = chunk.get(k);
    if (column->is_optional()) {
      is_optional = true;
      break;
    }
  }
  if (is_optional) {
    gs::runtime::OptionalGeneralPathColumnBuilder builder;
    auto arena = std::make_shared<gs::runtime::Arena>();
    for (const auto& chunk : chunks) {
      auto column = chunk.get(k);
      auto casted_col = dynamic_cast<const GeneralPathColumn*>(column);
      copy_optional_path_column_data(builder, *casted_col, chunk.offsets(),
                                     TABLE_ID(chunk.alias_map().at(k)));
      arena->emplace_back(
          std::make_unique<gs::runtime::ArenaRef>(casted_col->get_arena()));
    }
    return builder.finish(arena);
  } else {
    gs::runtime::GeneralPathColumnBuilder builder;
    auto arena = std::make_shared<gs::runtime::Arena>();
    for (const auto& chunk : chunks) {
      auto column = chunk.get(k);
      auto casted_col = dynamic_cast<const GeneralPathColumn*>(column);
      copy_path_column_data(builder, *casted_col, chunk.offsets(),
                            TABLE_ID(chunk.alias_map().at(k)));
      arena->emplace_back(
          std::make_unique<gs::runtime::ArenaRef>(casted_col->get_arena()));
    }
    return builder.finish(arena);
  }
}

std::shared_ptr<gs::runtime::IContextColumn> create_vertex_column(
    const std::vector<DataChunk>& chunks, int k) {
  bool is_optional = false;
  std::unordered_set<label_t> labels_set;
  for (const auto& chunk : chunks) {
    auto column = chunk.get(k);

    if (column->is_optional()) {
      is_optional = true;
    }
    auto casted_col = dynamic_cast<const IVertexColumn*>(column);
    const auto& temp = casted_col->get_labels_set();
    labels_set.insert(temp.begin(), temp.end());
  }
  if (labels_set.size() == 1) {
    if (!is_optional) {
      auto builder =
          gs::runtime::SLVertexColumnBuilder::builder(*labels_set.begin());
      for (const auto& chunk : chunks) {
        auto column = dynamic_cast<const IVertexColumn*>(chunk.get(k));
        if (column->vertex_column_type() == VertexColumnType::kSingle) {
          auto casted_col = dynamic_cast<const SLVertexColumn*>(column);
          copy_sl_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                     TABLE_ID(chunk.alias_map().at(k)));
        } else {
          auto casted_col = dynamic_cast<const MLVertexColumn*>(column);
          copy_sl_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                     TABLE_ID(chunk.alias_map().at(k)));
        }
      }
      return builder.finish(nullptr);
    } else {
      auto builder = gs::runtime::SLVertexColumnBuilder::optional_builder(
          *labels_set.begin());
      for (const auto& chunk : chunks) {
        auto column = dynamic_cast<const IVertexColumn*>(chunk.get(k));
        if (column->vertex_column_type() == VertexColumnType::kSingle) {
          auto casted_col = dynamic_cast<const SLVertexColumn*>(column);
          copy_sl_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                     TABLE_ID(chunk.alias_map().at(k)));
        } else if (column->vertex_column_type() ==
                   VertexColumnType::kMultiple) {
          auto casted_col = dynamic_cast<const MLVertexColumn*>(column);
          copy_sl_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                     TABLE_ID(chunk.alias_map().at(k)));
        }
      }
      return builder.finish(nullptr);
    }
  } else {
    if (!is_optional) {
      auto builder = gs::runtime::MLVertexColumnBuilder::builder(labels_set);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const MLVertexColumn*>(column);

        copy_ml_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else {
      auto builder =
          gs::runtime::MLVertexColumnBuilder::optional_builder(labels_set);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const MLVertexColumn*>(column);
        copy_ml_vertex_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    }
  }
  return nullptr;
}

std::shared_ptr<gs::runtime::IContextColumn> create_edge_column(
    const std::vector<DataChunk>& chunks, int k) {
  bool is_optional = false;
  gs::runtime::EdgeColumnType edge_type = gs::runtime::EdgeColumnType::kUnKnown;
  std::vector<std::pair<LabelTriplet, gs::PropertyType>> labels_and_types;
  Direction dir = Direction::kBoth;
  for (const auto& chunk : chunks) {
    auto column = chunk.get(k);
    if (column->is_optional()) {
      is_optional = true;
    }
    auto casted_col = dynamic_cast<const IEdgeColumn*>(column);
    if (edge_type == gs::runtime::EdgeColumnType::kUnKnown) {
      if (casted_col->edge_column_type() == EdgeColumnType::kBDML) {
        edge_type = gs::runtime::EdgeColumnType::kBDML;
        auto bdml_col = dynamic_cast<const BDMLEdgeColumn*>(casted_col);
        labels_and_types = bdml_col->edge_labels_;
      } else if (casted_col->edge_column_type() == EdgeColumnType::kBDSL) {
        edge_type = gs::runtime::EdgeColumnType::kBDSL;
        auto bdsl_col = dynamic_cast<const BDSLEdgeColumn*>(casted_col);
        labels_and_types.emplace_back(bdsl_col->get_labels()[0],
                                      bdsl_col->prop_type_);
      } else if (casted_col->edge_column_type() == EdgeColumnType::kSDML) {
        edge_type = gs::runtime::EdgeColumnType::kSDML;

        auto sdml_col = dynamic_cast<const SDMLEdgeColumn*>(casted_col);
        dir = sdml_col->dir_;
        labels_and_types = sdml_col->edge_labels_;
      } else if (casted_col->edge_column_type() == EdgeColumnType::kSDSL) {
        edge_type = gs::runtime::EdgeColumnType::kSDSL;
        auto sdsl_col = dynamic_cast<const SDSLEdgeColumn*>(casted_col);
        dir = sdsl_col->dir_;
        labels_and_types.emplace_back(sdsl_col->label_, sdsl_col->prop_type());
      }
    }
  }
  if (!is_optional) {
    if (edge_type == gs::runtime::EdgeColumnType::kSDSL) {
      auto builder = gs::runtime::SDSLEdgeColumnBuilder::builder(
          dir, labels_and_types[0].first, labels_and_types[0].second);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const SDSLEdgeColumn*>(column);
        copy_sdsl_edge_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else if (edge_type == gs::runtime::EdgeColumnType::kSDML) {
      auto builder =
          gs::runtime::SDMLEdgeColumnBuilder::builder(dir, labels_and_types);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const SDMLEdgeColumn*>(column);
        copy_sdml_edge_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else if (edge_type == gs::runtime::EdgeColumnType::kBDSL) {
      auto builder = gs::runtime::BDSLEdgeColumnBuilder::builder(
          labels_and_types[0].first, labels_and_types[0].second);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const BDSLEdgeColumn*>(column);
        copy_bdsl_edge_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else if (edge_type == gs::runtime::EdgeColumnType::kBDML) {
      auto builder =
          gs::runtime::BDMLEdgeColumnBuilder::builder(labels_and_types);
      for (const auto& chunk : chunks) {
        auto column = chunk.get(k);
        auto casted_col = dynamic_cast<const BDMLEdgeColumn*>(column);
        copy_bdml_edge_column_data(builder, *casted_col, chunk.offsets(),
                                   TABLE_ID(chunk.alias_map().at(k)));
      }
      return builder.finish(nullptr);
    } else {
      LOG(FATAL) << "Unsupported edge column type: "
                 << static_cast<int>(edge_type);
    }
  }
  return nullptr;
}

void Converter::build_context(const std::vector<DataChunk>& chunks,
                              gs::runtime::Context& ctx) {
  ctx.clear();
  if (chunks.empty()) {
    build_empty_context(ctx);
    return;
  }

  for (auto& [k, v] : ctx_meta_.metas()) {
    const auto& [type, elem_type] = v;
    if (type == gs::runtime::ContextColumnType::kVertex) {
      auto ptr = create_vertex_column(chunks, k);
      ctx.set(k, ptr);
    } else if (type == gs::runtime::ContextColumnType::kEdge) {
      auto ptr = create_edge_column(chunks, k);
      ctx.set(k, ptr);
    } else if (type == gs::runtime::ContextColumnType::kPath) {
      auto ptr = create_path_column(chunks, k);
      ctx.set(k, ptr);
    } else {
      auto ptr = create_value_column(chunks, k, elem_type);
      ctx.set(k, ptr);
    }
  }
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs