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

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"

namespace gs {
namespace chunked_runtime {

std::shared_ptr<IContextColumn> ValueColumn<List>::shuffle(
    const ValueColumn<size_t>& offsets, bool shift) {
  auto ptr = std::make_shared<ValueColumn<List>>(elem_type_);

  if (!shift) {
    for (size_t i = 0; i < size_; ++i) {
      ptr->data_[i] = data_[offsets[i] & 0xFFFFFFFF];
    }
  } else {
    for (size_t i = 0; i < size_; ++i) {
      ptr->data_[i] = data_[offsets[i] >> 32];
    }
  }

  ptr->size_ = size_;
  ptr->arena_ = arena_;
  return ptr;
}
}  // namespace chunked_runtime
}  // namespace gs
