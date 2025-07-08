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

#define TABLE_ID(idx) ((idx) >> 16)
#define COLUMN_ID(idx) ((idx) & 0xFFFF)
#define GLOBAL_COLUMN_ID(table_id, column_id) \
  ((table_id << 16) | (column_id & 0xFFFF))
class Table {
 public:
  Table() {}
  ~Table() = default;

  // Get the number of rows in the table
  size_t row_num() const {
    if (columns_.size() == 0) {
      return 0;
    }
    return columns_[0]->size();
  }

  // Get the number of columns in the table
  size_t col_num() const { return columns_.size(); }

  // Get a column by its index
  IContextColumn* get(uint32_t index) const { return columns_[index].get(); }

  // Clear the table
  void clear() { columns_.clear(); }

  void copy_from(Table& table, uint32_t table_id,
                 const std::unordered_map<uint32_t, int32_t>& revert_map,
                 std::unordered_map<int32_t, uint32_t>& alias_map) {
    for (uint32_t i = 0; i < static_cast<uint32_t>(table.columns_.size());
         ++i) {
      uint32_t idx = GLOBAL_COLUMN_ID(table_id, i);
      if (revert_map.at(idx) == -1) {
        continue;
      }
      alias_map[revert_map.at(idx)] =
          GLOBAL_COLUMN_ID(table_id, columns_.size());
      columns_.emplace_back(table.columns_[i]);
    }
  }

  void shuffle(const ValueColumn<size_t>& offsets, bool shift = false) {
    for (auto& column : columns_) {
      column = column->shuffle(offsets, shift);
    }
  }
  void push_back(const std::shared_ptr<IContextColumn>& column) {
    columns_.emplace_back(column);
  }

  std::vector<std::shared_ptr<IContextColumn>> columns_;
};
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_TABLE_H_