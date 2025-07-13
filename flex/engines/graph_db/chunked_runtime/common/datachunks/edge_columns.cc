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

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"

namespace gs {
namespace chunked_runtime {

template <typename T>
std::shared_ptr<IContextColumn> project_impl(
    const gs::runtime::GraphReadInterface&, const IEdgeColumn& col,
    const std::string& property_name) {
  auto ptr = std::make_shared<ValueColumn<T>>(col.get_local_pool());
  if (!col.is_optional()) {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        col.foreach_edge([&ptr](size_t idx, const LabelTriplet& label,
                                vid_t src, vid_t dst, const EdgeData& data,
                                Direction dir) {
          ptr->push_back(static_cast<int32_t>(label.edge_label));
        });
      }
      return ptr;
    }
    col.foreach_edge([&ptr](size_t idx, const LabelTriplet& label, vid_t src,
                            vid_t dst, const EdgeData& data,
                            Direction dir) { ptr->push_back(data.as<T>()); });
  } else {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        col.foreach_edge([&ptr](size_t idx, const LabelTriplet& label,
                                vid_t src, vid_t dst, const EdgeData& data,
                                Direction dir) {
          if (src != std::numeric_limits<vid_t>::max() ||
              dst != std::numeric_limits<vid_t>::max()) {
            ptr->push_back(static_cast<int32_t>(label.edge_label));
          } else {
            ptr->push_back_null();
          }
        });
      }
      return ptr;
    }
    col.foreach_edge([&ptr](size_t idx, const LabelTriplet& label, vid_t src,
                            vid_t dst, const EdgeData& data, Direction dir) {
      if (src != std::numeric_limits<vid_t>::max() ||
          dst != std::numeric_limits<vid_t>::max()) {
        ptr->push_back(data.as<T>());
      } else {
        ptr->push_back_null();
      }
    });
  }
  return ptr;
}

std::shared_ptr<IContextColumn> IEdgeColumn::project(
    const gs::runtime::GraphReadInterface& graph,
    const std::string& property_name, const RTAnyType& type) {
  if (type == RTAnyType::kI32Value) {
    return project_impl<int32_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kI64Value) {
    return project_impl<int64_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kStringValue) {
    return project_impl<std::string_view>(graph, *this, property_name);
  } else if (type == RTAnyType::kDate32) {
    return project_impl<Day>(graph, *this, property_name);
  } else if (type == RTAnyType::kTimestamp) {
    return project_impl<Date>(graph, *this, property_name);
  }
  return nullptr;
}

const LocalMemPool& IEdgeColumn::get_local_pool() const {
  if (this->edge_column_type() == EdgeColumnType::kSDSL) {
    return static_cast<const SDSLEdgeColumn*>(this)->mem_pool;
  } else if (this->edge_column_type() == EdgeColumnType::kSDML) {
    return static_cast<const SDMLEdgeColumn*>(this)->local_pool_;
  } else if (this->edge_column_type() == EdgeColumnType::kBDSL) {
    return static_cast<const BDSLEdgeColumn*>(this)->local_pool_;
  } else {
    return static_cast<const BDMLEdgeColumn*>(this)->local_pool_;
  }
}
std::shared_ptr<IContextColumn> SDSLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr =
      std::make_shared<SDSLEdgeColumn>(mem_pool, dir_, label_, prop_type_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  size_t offset_size = offsets.size();
  if (!shift) {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      ptr->prop_col_.set_any(i, prop_col_, offsets[i] & 0xFFFFFFFF);
    }
  } else {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      ptr->prop_col_.set_any(i, prop_col_, offsets[i] >> 32);
    }
  }
  ptr->size_ = offset_size;
  return ptr;
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<SDMLEdgeColumn>(local_pool_, dir_, edge_labels_);
  size_t offset_size = offsets.size();
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  if (!shift) {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  } else {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  }
  ptr->size_ = offset_size;
  return ptr;
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<BDSLEdgeColumn>(local_pool_, label_, prop_type_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  size_t offset_size = offsets.size();
  if (!shift) {
    for (size_t i = 0; i < offset_size; ++i) {
      auto offset = offsets[i] & 0xFFFFFFFF;
      ptr->edges_[i] = edges_[offset];
      ptr->prop_col_.set_any(i, prop_col_, offset);
    }
  } else {
    for (size_t i = 0; i < offset_size; ++i) {
      auto offset = offsets[i] >> 32;
      ptr->edges_[i] = edges_[offset];
      ptr->prop_col_.set_any(i, prop_col_, offset);
    }
  }
  ptr->size_ = offset_size;
  return ptr;
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<BDMLEdgeColumn>(local_pool_, edge_labels_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  size_t offset_size = offsets.size();

  if (!shift) {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  } else {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  }

  ptr->size_ = offset_size;
  return ptr;
}

}  // namespace chunked_runtime
}  // namespace gs