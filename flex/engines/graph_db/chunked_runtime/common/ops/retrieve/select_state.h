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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SELECT_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SELECT_STATE_H_
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
struct LocalSelectState {
  LocalSelectState(const LocalMemPool& mem_pool)
      : cur_idx_(0), mem_pool_(mem_pool), cur_column_(nullptr) {
    columns.emplace_back(std::make_shared<ValueColumn<size_t>>(mem_pool_));
    cur_column_ = columns.back().get();
  }
  void push_back(size_t idx) {
    cur_column_->push_back(idx);
    if (cur_column_->full()) {
      columns.emplace_back(std::make_shared<ValueColumn<size_t>>(mem_pool_));
      cur_column_ = columns.back().get();
    }
  }
  int cur_idx_;
  const LocalMemPool& mem_pool_;
  ValueColumn<size_t>* cur_column_;
  std::vector<std::shared_ptr<ValueColumn<size_t>>> columns;
};
class SelectState : public IOprState {
 public:
  SelectState(std::shared_ptr<IOprState> src_state, int table_id,
              const LocalMemPool& mem_pool)
      : cur_idx_(0),
        src_state_(std::move(src_state)),
        table_id_(table_id),
        initialized_(false),

        mem_pool_(mem_pool) {}

  void clear() override {
    local_states_.clear();
    cur_idx_ = 0;
    source_chunks.clear();
  }

  void initialize() { initialized_ = true; }

  LocalSelectState& getLocalState() {
    local_states_.emplace_back(mem_pool_);
    return local_states_.back();
  }

  DataChunks& src_chunks() { return source_chunks; }

  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  bool initialized() const override { return initialized_; }

  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= static_cast<int>(local_states_.size())) {
      return false;
    }
    bool flag = false;
    for (int i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= static_cast<int>(local_states_.size())) {
          break;
        }
        if (static_cast<int>(local_states_[cur_idx_].columns.size()) <=
            local_states_[cur_idx_].cur_idx_) {
          cur_idx_++;
          continue;
        }
        if (local_states_[cur_idx_]
                .columns[local_states_[cur_idx_].cur_idx_]
                ->size() == 0) {
          local_states_[cur_idx_].cur_idx_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto column =
            local_states_[cur_idx_].columns[local_states_[cur_idx_].cur_idx_];
        auto& source_chunk = source_chunks[cur_idx_];
        chunks.emplace_back(DataChunk::filter(source_chunk, column, table_id_));
        local_states_[cur_idx_].cur_idx_++;
        break;
      }
    }

    return flag;
  }

 private:
  DataChunks source_chunks;
  int cur_idx_;
  std::shared_ptr<IOprState> src_state_;
  int table_id_;
  std::vector<LocalSelectState> local_states_;
  bool initialized_ = false;

  const LocalMemPool& mem_pool_;
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SELECT_STATE_H_