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
  auto ptr = std::make_shared<SLVertexColumn>(label_);
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

std::shared_ptr<IContextColumn> MLVertexColumn::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto new_column = std::make_shared<MLVertexColumn>(labels_);
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