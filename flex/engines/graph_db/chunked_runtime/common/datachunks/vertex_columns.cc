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

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
namespace gs {
namespace chunked_runtime {
std::shared_ptr<IContextColumn> SLVertexColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<SLVertexColumn>(local_pool_, label_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  size_t offset_size = offsets.size();
  if (!shift) {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->data_[i] = data_[offsets[i] & 0xFFFFFFFF];
    }
  } else {
    for (size_t i = 0; i < offset_size; ++i) {
      ptr->data_[i] = data_[offsets[i] >> 32];
    }
  }
  ptr->size_ = offset_size;
  return ptr;
}

template <typename T>
std::shared_ptr<IContextColumn> project_impl(
    const gs::runtime::GraphReadInterface& graph, const SLVertexColumn& vertex,
    const std::string& property_name) {
  auto ptr = std::make_shared<ValueColumn<T>>(vertex.local_pool_);
  if (!vertex.is_optional_) {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        for (size_t i = 0; i < vertex.size(); ++i) {
          ptr->push_back(static_cast<T>(vertex.label_));
        }
        return ptr;
      }
    }

    const auto& property_col =
        graph.GetVertexColumn<T>(vertex.label_, property_name);
    if (property_col.is_null()) {
      for (size_t i = 0; i < vertex.size(); ++i) {
        ptr->push_back_null();
      }
      return ptr;
    }
    for (size_t i = 0; i < vertex.size(); ++i) {
      ptr->push_back(property_col.get_view(vertex.data_[i]));
    }

  } else {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        for (size_t i = 0; i < vertex.size(); ++i) {
          if (vertex.has_value(i)) {
            ptr->push_back(static_cast<T>(vertex.label_));
          } else {
            ptr->push_back_null();
          }
        }
        return ptr;
      }
    }
    const auto& property_col =
        graph.GetVertexColumn<T>(vertex.label_, property_name);
    if (property_col.is_null()) {
      for (size_t i = 0; i < vertex.size(); ++i) {
        ptr->push_back_null();
      }
      return ptr;
    }
    for (size_t i = 0; i < vertex.size(); ++i) {
      if (vertex.has_value(i)) {
        ptr->push_back(property_col.get_view(vertex.data_[i]));
      } else {
        ptr->push_back_null();
      }
    }
  }
  return ptr;
}

std::shared_ptr<IContextColumn> SLVertexColumn::project(
    const gs::runtime::GraphReadInterface& graph,
    const std::string& property_name, const RTAnyType& type) {
  if (type == RTAnyType::kI64Value) {
    return project_impl<int64_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kI32Value) {
    return project_impl<int32_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kDate32) {
    return project_impl<Day>(graph, *this, property_name);
  } else if (type == RTAnyType::kStringValue) {
    return project_impl<std::string_view>(graph, *this, property_name);
  } else if (type == RTAnyType::kTimestamp) {
    return project_impl<Date>(graph, *this, property_name);
  } else {
    LOG(FATAL) << "Project not implemented for this column type: "
               << to_string(static_cast<int>(type));
  }
  return nullptr;
}

template <typename T>
std::shared_ptr<IContextColumn> project_impl(
    const gs::runtime::GraphReadInterface& graph, const MLVertexColumn& vertex,
    const std::string& property_name) {
  auto ptr = std::make_shared<ValueColumn<T>>(vertex.local_pool_);
  using vertex_column_t =
      typename gs::runtime::GraphReadInterface::template vertex_column_t<T>;

  if (!vertex.is_optional_) {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        for (size_t i = 0; i < vertex.size(); ++i) {
          ptr->push_back(static_cast<T>(vertex.data_[i].label_));
        }
        return ptr;
      }
    }
    std::vector<vertex_column_t> property_columns(
        graph.schema().vertex_label_num());
    for (auto label : vertex.get_labels_set()) {
      auto col = graph.GetVertexColumn<T>(label, property_name);
      property_columns[label] = col;
    }
    for (size_t i = 0; i < vertex.size(); ++i) {
      auto label = vertex.data_[i].label_;
      auto vid = vertex.data_[i].vid_;
      if (!property_columns[label].is_null()) {
        ptr->push_back(property_columns[label].get_view(vid));
      } else {
        ptr->push_back_null();
      }
    }

  } else {
    if constexpr (std::is_same_v<T, int32_t>) {
      if (property_name == "label") {
        for (size_t i = 0; i < vertex.size(); ++i) {
          if (vertex.has_value(i)) {
            ptr->push_back(static_cast<T>(vertex.data_[i].label_));
          } else {
            ptr->push_back_null();
          }
        }
        return ptr;
      }
    }
    std::vector<vertex_column_t> property_columns(
        graph.schema().vertex_label_num());
    for (auto label : vertex.get_labels_set()) {
      auto col = graph.GetVertexColumn<T>(label, property_name);
      property_columns[label] = col;
    }
    for (size_t i = 0; i < vertex.size(); ++i) {
      auto label = vertex.data_[i].label_;
      auto vid = vertex.data_[i].vid_;
      if (vertex.has_value(i) && !property_columns[label].is_null()) {
        ptr->push_back(property_columns[label].get_view(vid));
      } else {
        ptr->push_back_null();
      }
    }
  }
  return ptr;
}

std::shared_ptr<IContextColumn> MLVertexColumn::project(
    const gs::runtime::GraphReadInterface& graph,
    const std::string& property_name, const RTAnyType& type) {
  if (type == RTAnyType::kI64Value) {
    return project_impl<int64_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kI32Value) {
    return project_impl<int32_t>(graph, *this, property_name);
  } else if (type == RTAnyType::kDate32) {
    return project_impl<Day>(graph, *this, property_name);
  } else if (type == RTAnyType::kStringValue) {
    return project_impl<std::string_view>(graph, *this, property_name);
  } else if (type == RTAnyType::kTimestamp) {
    return project_impl<Date>(graph, *this, property_name);
  } else {
    LOG(FATAL) << "Project not implemented for this column type: "
               << to_string(static_cast<int>(type));
  }
  return nullptr;
}

std::shared_ptr<IContextColumn> MLVertexColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto new_column = std::make_shared<MLVertexColumn>(local_pool_, labels_);
  int offset_size = offsets.size();
  new_column->is_optional_ = is_optional_;
  new_column->size_ = offset_size;
  if (!shift) {
    for (int i = 0; i < offset_size; ++i) {
      new_column->data_[i] = data_[offsets[i] & 0xFFFFFFFF];
    }
  } else {
    for (int i = 0; i < offset_size; ++i) {
      new_column->data_[i] = data_[offsets[i] >> 32];
    }
  }

  return new_column;
}

}  // namespace chunked_runtime
}  // namespace gs