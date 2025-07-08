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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_STATE_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunks.h"
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
template <typename ColT>
class GetVCollector;

class GetVOffsetCollector {
 public:
  GetVOffsetCollector(ValueColumn<size_t>& offsets) : offsets_(offsets) {}

  void push_back(size_t index) { offsets_.push_back(index); }

  IVertexColumn* col_;
  ValueColumn<size_t>& offsets_;
};

struct LocalGetVState {
  template <typename ColT, typename... Args>
  GetVCollector<ColT> getVertexCollector(const Args&... args) {
    column = std::make_shared<ColT>(args...);
    offsets.clear();
    return GetVCollector<ColT>(column, offsets);
  }

  GetVOffsetCollector getOffsetCollector() {
    column = nullptr;
    offsets.clear();
    return GetVOffsetCollector(offsets);
  }
  std::shared_ptr<IVertexColumn> column;
  ValueColumn<size_t> offsets;
};

template <typename ColT>
class GetVCollector {
 public:
  GetVCollector(std::shared_ptr<IVertexColumn> column,
                ValueColumn<size_t>& offsets)
      : col_(dynamic_cast<ColT*>(column.get())), offsets_(offsets) {}

  template <typename... Params>
  void push_back(size_t index, Params&&... params) {
    offsets_.push_back(index);
    col_->push_back_opt(std::forward<Params>(params)...);
  }

  void push_back_null(size_t index) {
    offsets_.push_back(index);
    col_->push_back_null();
  }

  ColT* col_;
  ValueColumn<size_t>& offsets_;
};

struct GetVState : public IOprState {
  GetVState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(std::move(src_state)) {}

  void clear() override {
    local_states_.clear();
    src_chunks_.clear();
    cur_idx_ = 0;
  }
  bool initialized() const override { return initialized_; }
  DataChunks& src_chunks() override { return src_chunks_; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }

  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= local_states_.size()) {
      return false;
    }
    bool flag = false;
    for (size_t i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= local_states_.size()) {
          break;  // No more local states to process
        }

        if (local_states_[cur_idx_].offsets.size() == 0) {
          cur_idx_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto& local_state = local_states_[cur_idx_];
        auto& vertex_col = local_state.column;
        auto& offsets = local_state.offsets;
        if (vertex_col == nullptr) {
          chunks.emplace_back(DataChunk::create(src_chunks_[cur_idx_], offsets,
                                                src_table_, alias_));
        } else {
          chunks.emplace_back(DataChunk::create(
              src_chunks_[cur_idx_], vertex_col, offsets, src_table_, alias_));
        }
        cur_idx_++;
        break;
      }
    }

    return flag;
  }

  void initialize(int src_table, int alias) {
    src_table_ = src_table;
    alias_ = alias;
    cur_idx_ = 0;
    initialized_ = true;
    local_states_.clear();
  }

  LocalGetVState& getLocalState() {
    local_states_.emplace_back();
    return local_states_.back();
  }

  std::vector<LocalGetVState> local_states_;

  bool initialized_;
  DataChunks src_chunks_;
  std::shared_ptr<IOprState> src_state_;
  int src_table_;
  int alias_;
  size_t cur_idx_;
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_GET_V_STATE_H_