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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_TABLE_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_TABLE_H_
#include <memory>
#include <vector>

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/i_context_column.h"
namespace gs {
namespace chunked_runtime {
class Table {
 public:
  Table() : col_num_(0) {}
  ~Table() = default;

  // Get the number of rows in the table
  size_t row_num() const {
    if (col_num_ == 0) {
      return 0;
    }
    return columns_[0]->size();
  }

  // Get the number of columns in the table
  size_t col_num() const { return col_num_; }

  // Get a column by its index
  IContextColumn* get(size_t index) const {
    return columns_[index & 0xFFFFFFFFF].get();
  }

  // Clear the table
  void clear() {
    col_num_ = 0;
    for (auto& column : columns_) {
      column->clear();
    }
  }

  size_t col_num_;
  std::vector<std::shared_ptr<IContextColumn>> columns_;
};
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_TABLE_H_