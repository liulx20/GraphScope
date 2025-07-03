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
std::shared_ptr<IContextColumn> SDSLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<SDSLEdgeColumn>(dir_, label_, prop_type_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  if (!shift) {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      ptr->prop_col_.set_any(i, prop_col_, offsets[i] & 0xFFFFFFFF);
    }
  } else {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      ptr->prop_col_.set_any(i, prop_col_, offsets[i] >> 32);
    }
  }
  ptr->size_ = size_;
  return ptr;
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<SDMLEdgeColumn>(dir_, edge_labels_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  if (!shift) {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  } else {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  }
  ptr->size_ = size_;
  return ptr;
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<BDSLEdgeColumn>(label_, prop_type_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }
  if (!shift) {
    for (size_t i = 0; i < size_; ++i) {
      auto offset = offsets[i] & 0xFFFFFFFF;
      ptr->edges_[i] = edges_[offset];
      ptr->prop_col_.set_any(i, prop_col_, offset);
    }
  } else {
    for (size_t i = 0; i < size_; ++i) {
      auto offset = offsets[i] >> 32;
      ptr->edges_[i] = edges_[offset];
      ptr->prop_col_.set_any(i, prop_col_, offset);
    }
  }
  ptr->size_ = size_;
  return ptr;
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<BDMLEdgeColumn>(edge_labels_);
  if (is_optional_) {
    ptr->is_optional_ = true;
  }

  if (!shift) {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] & 0xFFFFFFFF];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  } else {
    for (size_t i = 0; i < size_; ++i) {
      ptr->edges_[i] = edges_[offsets[i] >> 32];
      auto index = std::get<0>(ptr->edges_[i]);
      auto offset = std::get<3>(ptr->edges_[i]);
      size_t new_offset = ptr->prop_cols_[index].size();
      ptr->prop_cols_[index].set_any(new_offset, prop_cols_[index], offset);
      std::get<3>(ptr->edges_[i]) = new_offset;
    }
  }

  ptr->size_ = size_;
  return ptr;
}

}  // namespace chunked_runtime
}  // namespace gs