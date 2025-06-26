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
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/table.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
namespace gs {
namespace chunked_runtime {
class DataChunk {
 public:
  DataChunk() : leaves_size_(0) {}

  void clear() {
    table_->clear();
    leaves_size_ = 0;
    for (auto& leaf : leaves_) {
      leaf->clear();
    }
  }

  size_t row_num() const {
    size_t row_num = table_->row_num();
    size_t cnt = 0;
    for (size_t i = 0; i < row_num; ++i) {
      size_t cur = 1;
      const auto& offsets = offsets_[i];
      for (size_t j = 0; j < leaves_size_; ++j) {
        cur *= (offsets[j] >> 32);
      }
      cnt += cur;
    }
    return cnt;
  }

  bool is_optional(size_t idx) const {
    uint32_t table_id = idx >>= 32;
    uint32_t column_id = idx & 0xFFFFFFFF;
    if (table_id == 0) {
      return table_->get(column_id)->is_optional();
    } else {
      CHECK_LT(table_id, leaves_size_);
      return leaves_[table_id]->get(column_id)->is_optional();
    }
  }

  // vertex column infos

  std::unordered_set<label_t> get_vertex_labels_set(size_t idx) const {
    return dynamic_cast<IVertexColumn*>(get(idx))->get_labels_set();
  }

  VertexColumnType get_vertex_column_type(size_t idx) const {
    return dynamic_cast<IVertexColumn*>(get(idx))->vertex_column_type();
  }

  template <typename FUNC_T>
  void foreach_vertex(size_t v_tag, const FUNC_T& func) const {}

  // edge column infos
  std::vector<LabelTriplet> get_edge_labels(size_t idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->get_labels();
  }

  EdgeColumnType get_edge_column_type(size_t idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->edge_column_type();
  }

  template <typename FUNC_T>
  void foreach_edge(size_t v_tag, const FUNC_T& func) const {}

  ContextColumnType get_column_type(size_t idx) const {
    return get(idx)->column_type();
  }

  Direction get_edge_direction(size_t idx) const {
    return dynamic_cast<IEdgeColumn*>(get(idx))->dir();
  }

  template <typename FUNC_T>
  void foreach_path(size_t index, const FUNC_T& func) const {}

  inline IContextColumn* get(size_t idx) const {
    uint32_t table_id = idx >>= 32;
    uint32_t column_id = idx & 0xFFFFFFFF;
    if (table_id == 0) {
      return table_->get(column_id);
    } else {
      CHECK_LT(table_id, leaves_size_);
      return leaves_[table_id]->get(column_id);
    }
  }

 private:
  size_t leaves_size_;
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<Table>> leaves_;
  std::vector<ValueColumn<size_t>> offsets_;
};

}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNK_H_