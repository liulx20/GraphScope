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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNK_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNK_H_
#include <memory>

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/path_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/table.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
namespace gs {
namespace chunked_runtime {

class DataChunk {
 public:
  /**void info() const {
    std::stringstream ss;
    for (const auto& [alias, idx] : alias_map_) {
      ss << "(Alias: " << alias << ", Index: " << idx << ") \n";
    }
    for (size_t i = 0; i < table_->columns_.size(); ++i) {
      const auto& col = table_->columns_[i];
      ss << "(0," << i << ")" << col->column_info() << ", ";
    }
    ss << "\n";
    for (size_t i = 0; i < leaves_.size(); ++i) {
      for (size_t j = 0; j < leaves_[i]->columns_.size(); ++j) {
        const auto& col = leaves_[i]->columns_[j];
        ss << "(" << (i + 1) << "," << j << ")" << col->column_info() << ", ";
      }
      ss << "\n";
    }
    LOG(INFO) << "DataChunk info: table size: " << table_->col_num()
              << ", alias_map size: " << alias_map_.size()
              << ", leaves size: " << leaves_.size()
              << ", offsets size: " << offsets_.size() << " \n"
              << ss.str()
              << "END\n====================================================";
  }*/
  static DataChunk create(const std::shared_ptr<IContextColumn>& column,
                          int alias) {
    DataChunk chunk;
    chunk.table_ = std::make_shared<Table>();
    chunk.table_->columns_.emplace_back(column);
    chunk.alias_map_[alias] = 0;
    if (alias != -1) {
      chunk.alias_map_[-1] = 0;
    }
    return chunk;
  }

  std::unordered_map<uint32_t, int32_t> get_revert_map() const {
    std::unordered_map<uint32_t, int32_t> revert_map;
    size_t sp_tag = std::numeric_limits<size_t>::max();
    for (const auto& pair : alias_map_) {
      if (pair.first == -1) {
        sp_tag = pair.second;
      } else {
        revert_map[pair.second] = pair.first;
      }
    }
    if (sp_tag != std::numeric_limits<size_t>::max() &&
        revert_map.find(sp_tag) == revert_map.end()) {
      revert_map[sp_tag] = -1;
    }
    return revert_map;
  }

  static std::shared_ptr<ValueColumn<size_t>> generate_leaves_offsets(
      const ValueColumn<size_t>& offsets, size_t len) {
    auto leaves_offsets =
        std::make_shared<ValueColumn<size_t>>(offsets.local_pool_);
    size_t cur_offset = 0;
    leaves_offsets->emplace_back(cur_offset << 32);
    for (size_t i = 0; i < offsets.size(); ++i) {
      while ((offsets[i] >> 32) != cur_offset) {
        cur_offset++;
        leaves_offsets->emplace_back(i << 32);
      }
      (*leaves_offsets)[cur_offset] += 1;
    }
    for (size_t i = cur_offset + 1; i < len; ++i) {
      leaves_offsets->emplace_back(i << 32);
    }
    return leaves_offsets;
  }
  static DataChunk create(DataChunk& other,
                          std::shared_ptr<ValueColumn<size_t>>& offsets,
                          int src_table_id, int alias) {
    DataChunk chunk;
    chunk.table_ = std::make_shared<Table>();
    src_table_id = TABLE_ID(other.alias_map_.at(src_table_id));
    const auto& revert_map = other.get_revert_map();
    chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
    for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
      chunk.leaves_.emplace_back(std::make_shared<Table>());
      chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                  chunk.alias_map_);
      chunk.offsets_.emplace_back(other.offsets_[i]);
    }
    if (src_table_id == 0) {
      CHECK(src_table_id != 0) << "src_table_id should not be 0";

    } else {
      chunk.leaves_[src_table_id - 1]->shuffle(*offsets);
      chunk.offsets_[src_table_id - 1]->shuffle(*offsets, false);
      if (offsets->size() != other.leaves_[src_table_id - 1]->row_num()) {
        chunk.offsets_[src_table_id - 1] = generate_leaves_offsets(
            *offsets, chunk.offsets_[src_table_id - 1]->size());
      }
    }
    return chunk;
  }

  static DataChunk create(DataChunk& other,
                          std::shared_ptr<IContextColumn> column,
                          std::shared_ptr<ValueColumn<size_t>>& offsets,
                          int src_table_id, int alias) {
    DataChunk chunk;
    chunk.table_ = std::make_shared<Table>();
    src_table_id = TABLE_ID(other.alias_map_.at(src_table_id));
    const auto& revert_map = other.get_revert_map();
    chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
    for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
      chunk.leaves_.emplace_back(std::make_shared<Table>());
      chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                  chunk.alias_map_);
      chunk.offsets_.emplace_back(other.offsets_[i]);
    }

    if (src_table_id == 0) {
      CHECK(src_table_id != 0) << "src_table_id should not be 0";
      /**
      chunk.table_->shuffle(offsets);
      chunk.table_->columns_.emplace_back(column);
      chunk.alias_map_[alias] =
          GLOBAL_COLUMN_ID(0, (chunk.table_->col_num() - 1));*/
    } else {
      chunk.leaves_[src_table_id - 1]->shuffle(*offsets);
      chunk.offsets_[src_table_id - 1]->shuffle(*offsets, false);
      chunk.leaves_[src_table_id - 1]->columns_.emplace_back(column);
      chunk.alias_map_[alias] = GLOBAL_COLUMN_ID(
          src_table_id, (chunk.leaves_[src_table_id - 1]->col_num() - 1));
      if (alias != -1) {
        chunk.alias_map_[-1] = GLOBAL_COLUMN_ID(
            src_table_id, (chunk.leaves_[src_table_id - 1]->col_num() - 1));
      }
      if (offsets->size() != other.leaves_[src_table_id - 1]->row_num()) {
        chunk.offsets_[src_table_id - 1] = generate_leaves_offsets(
            *offsets, chunk.offsets_[src_table_id - 1]->size());
      }
    }

    return chunk;
  }

  static DataChunk create(DataChunk& other,
                          std::shared_ptr<ValueColumn<size_t>>& offsets,
                          std::shared_ptr<ValueColumn<size_t>>& leaves_offsets,
                          std::shared_ptr<IContextColumn> column, int alias,
                          int src_table_id) {
    DataChunk chunk;
    chunk.table_ = std::make_shared<Table>();

    src_table_id = TABLE_ID(other.alias_map_.at(src_table_id));

    const auto& revert_map = other.get_revert_map();
    if (src_table_id == 0) {
      chunk.offsets_.resize(other.offsets_.size());
      for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
        chunk.leaves_.emplace_back(std::make_shared<Table>());
        chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                    chunk.alias_map_);
        chunk.offsets_[i] = other.offsets_[i];
      }
      chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
      chunk.table_->shuffle(*offsets);
      chunk.offsets_.emplace_back(leaves_offsets);
      chunk.leaves_.emplace_back(std::make_shared<Table>());
      chunk.leaves_.back()->push_back(column);
      chunk.alias_map_[alias] = GLOBAL_COLUMN_ID(chunk.offsets_.size(), 0);
      if (alias != -1) {
        chunk.alias_map_[-1] = GLOBAL_COLUMN_ID(chunk.offsets_.size(), 0);
      }
    } else {
      chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
      chunk.table_->shuffle(*offsets, true);
      chunk.offsets_.resize(other.offsets_.size());
      for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
        chunk.leaves_.emplace_back(std::make_shared<Table>());
        if (i + 1 == src_table_id) {
          continue;
        }
        chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                    chunk.alias_map_);
        chunk.offsets_[i] = other.offsets_[i];
        chunk.offsets_[i]->shuffle(*offsets, true);
      }
      for (int i = 0;
           i < static_cast<int>(other.leaves_[src_table_id - 1]->col_num());
           ++i) {
        uint32_t idx = GLOBAL_COLUMN_ID(src_table_id, i);
        int32_t v = revert_map.at(idx);
        if (v == -1) {
          continue;
        } else {
          auto col =
              other.leaves_[src_table_id - 1]->get(i)->shuffle(*offsets, false);
          chunk.table_->push_back(col);
          chunk.alias_map_[v] =
              GLOBAL_COLUMN_ID(0, (chunk.table_->col_num() - 1));
        }
      }
      chunk.offsets_[src_table_id - 1] = leaves_offsets;
      chunk.leaves_[src_table_id - 1]->push_back(column);
      chunk.alias_map_[alias] = GLOBAL_COLUMN_ID(src_table_id, 0);
      if (alias != -1) {
        chunk.alias_map_[-1] = GLOBAL_COLUMN_ID(src_table_id, 0);
      }
    }

    return chunk;
  }

  static DataChunk create(
      DataChunk& other, std::shared_ptr<ValueColumn<size_t>>& offsets,
      std::shared_ptr<ValueColumn<size_t>>& leaves_offsets,
      std::vector<std::pair<std::shared_ptr<IContextColumn>, int>> columns,
      int src_table_id) {
    DataChunk chunk;
    chunk.table_ = std::make_shared<Table>();
    src_table_id = TABLE_ID(other.alias_map_.at(src_table_id));
    const auto& revert_map = other.get_revert_map();
    if (src_table_id == 0) {
      for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
        chunk.leaves_.emplace_back(std::make_shared<Table>());
        chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                    chunk.alias_map_);
        chunk.offsets_.emplace_back(other.offsets_[i]);
      }
      chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
      chunk.table_->shuffle(*offsets);
      chunk.offsets_.emplace_back(leaves_offsets);
      chunk.leaves_.emplace_back(std::make_shared<Table>());
      size_t idx = 0;
      for (const auto& pair : columns) {
        chunk.leaves_.back()->push_back(pair.first);
        chunk.alias_map_[pair.second] =
            GLOBAL_COLUMN_ID(chunk.offsets_.size(), idx++);
      }
    } else {
      chunk.table_->copy_from(*other.table_, 0, revert_map, chunk.alias_map_);
      chunk.table_->shuffle(*offsets, true);
      for (int i = 0; i < static_cast<int>(other.leaves_.size()); ++i) {
        chunk.leaves_.emplace_back(std::make_shared<Table>());
        if (i + 1 == src_table_id) {
          continue;
        }
        chunk.leaves_[i]->copy_from(*other.leaves_[i], i + 1, revert_map,
                                    chunk.alias_map_);
        chunk.offsets_.emplace_back(other.offsets_[i]);
        chunk.offsets_[i]->shuffle(*offsets, true);
      }
      for (int i = 0;
           i < static_cast<int>(other.leaves_[src_table_id - 1]->col_num());
           ++i) {
        uint32_t idx = GLOBAL_COLUMN_ID(src_table_id, i);
        int32_t v = revert_map.at(idx);
        if (v == -1) {
          continue;
        } else {
          auto col =
              other.leaves_[src_table_id - 1]->get(i)->shuffle(*offsets, false);
          chunk.table_->push_back(col);
          chunk.alias_map_[v] =
              GLOBAL_COLUMN_ID(0, (chunk.table_->col_num() - 1));
        }
      }
      chunk.offsets_[src_table_id - 1] = leaves_offsets;
      for (const auto& pair : columns) {
        chunk.leaves_[src_table_id - 1]->push_back(pair.first);
        chunk.alias_map_[pair.second] = GLOBAL_COLUMN_ID(
            src_table_id, (chunk.leaves_[src_table_id - 1]->col_num() - 1));
      }
    }

    return chunk;
  }

  DataChunk() : table_(nullptr) {}

  void clear() {
    table_->clear();
    for (auto& leaf : leaves_) {
      leaf->clear();
    }
    alias_map_.clear();
    offsets_.clear();
  }

  size_t row_num() const {
    size_t row_num = table_->row_num();
    size_t cnt = 0;
    for (size_t i = 0; i < row_num; ++i) {
      size_t cur = 1;
      const auto& offsets = *offsets_[i];
      for (size_t j = 0; j < leaves_.size(); ++j) {
        cur *= (offsets[j] >> 32);
      }
      cnt += cur;
    }
    return cnt;
  }

  bool is_optional(int idx) const {
    idx = alias_map_.at(idx);
    uint32_t table_id = TABLE_ID(idx);
    uint32_t column_id = COLUMN_ID(idx);
    if (table_id == 0) {
      return table_->get(column_id)->is_optional();
    } else {
      return leaves_[table_id - 1]->get(column_id)->is_optional();
    }
  }

  // vertex column infos

  std::unordered_set<label_t> get_vertex_labels_set(int idx) const {
    return dynamic_cast<IVertexColumn*>(get(idx))->get_labels_set();
  }

  VertexColumnType get_vertex_column_type(int idx) const {
    return dynamic_cast<IVertexColumn*>(get(idx))->vertex_column_type();
  }

  template <typename FUNC_T>
  void foreach_vertex(int v_tag, const FUNC_T& func) const {
    uint32_t idx = alias_map_.at(v_tag);
    uint32_t table_id = TABLE_ID(idx);
    if (table_id == 0) {
      auto col = dynamic_cast<IVertexColumn*>(table_->get(COLUMN_ID(idx)));
      col->foreach_vertex(func);
    } else {
      auto col = dynamic_cast<IVertexColumn*>(
          leaves_[table_id - 1]->get(COLUMN_ID(idx)));
      col->foreach_vertex(func, *offsets_[table_id - 1]);
    }
  }

  // edge column infos
  std::vector<LabelTriplet> get_edge_labels(int idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->get_labels();
  }

  EdgeColumnType get_edge_column_type(int idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->edge_column_type();
  }

  template <typename FUNC_T>
  void foreach_edge(int v_tag, const FUNC_T& func) const {
    uint32_t idx = alias_map_.at(v_tag);
    uint32_t table_id = TABLE_ID(idx);
    if (table_id == 0) {
      auto col = dynamic_cast<IEdgeColumn*>(table_->get(COLUMN_ID(idx)));
      col->foreach_edge(func);
    } else {
      auto col = dynamic_cast<IEdgeColumn*>(
          leaves_[table_id - 1]->get(COLUMN_ID(idx)));
      col->foreach_edge(func, *offsets_[table_id - 1]);
    }
  }

  ContextColumnType get_column_type(int idx) const {
    return get(idx)->column_type();
  }

  Direction get_edge_direction(int idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->dir();
  }

  template <typename FUNC_T>
  void foreach_path(int index, const FUNC_T& func) const {
    uint32_t idx = alias_map_.at(index);
    uint32_t table_id = TABLE_ID(idx);
    if (table_id == 0) {
      auto col = dynamic_cast<IPathColumn*>(table_->get(COLUMN_ID(idx)));
      col->foreach_path(func);
    } else {
      auto col = dynamic_cast<IPathColumn*>(
          leaves_[table_id - 1]->get(COLUMN_ID(idx)));
      col->foreach_path(func, *offsets_[table_id - 1]);
    }
  }

  inline IContextColumn* get(int idx) const {
    idx = alias_map_.at(idx);
    uint32_t table_id = TABLE_ID(idx);
    uint32_t column_id = COLUMN_ID(idx);
    if (table_id == 0) {
      return table_->get(column_id);
    } else {
      return leaves_[table_id - 1]->get(column_id);
    }
  }

  const std::unordered_map<int32_t, uint32_t>& alias_map() const {
    return alias_map_;
  }

  const std::vector<std::shared_ptr<ValueColumn<size_t>>>& offsets() const {
    return offsets_;
  }

 private:
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<Table>> leaves_;
  std::vector<std::shared_ptr<ValueColumn<size_t>>> offsets_;
  std::unordered_map<int32_t, uint32_t> alias_map_;
};

}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNK_H_