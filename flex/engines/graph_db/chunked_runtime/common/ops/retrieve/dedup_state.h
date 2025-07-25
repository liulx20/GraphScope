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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_DEDUP_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_DEDUP_STATE_H_
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"

namespace gs {
namespace chunked_runtime {
namespace ops {

template <typename ColT, typename... Args>
class DedupCollector;
class DedupState : public IOprState {
 public:
  DedupState(std::shared_ptr<IOprState> src_state, const LocalMemPool& mem_pool)
      : initialized_(false), src_state_(src_state), local_pool_(mem_pool) {}

  void clear() override {
    cur_idx_ = 0;
    source_chunks.clear();
    columns.clear();  // Clear the columns
  }

  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= static_cast<int>(columns.size())) {
      return false;  // No more chunks to process
    }
    bool flag = false;
    for (size_t i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= static_cast<int>(columns.size())) {
          break;  // No more chunks to process
        }
        if (columns[cur_idx_]->size() > 0) {
          flag = true;
          chunks.emplace_back(DataChunk::create(columns[cur_idx_], alias_));
          ++cur_idx_;
          break;
        }
        cur_idx_++;
      }
    }
    return flag;
  }

  void initialize(int alias) {
    initialized_ = true;
    alias_ = alias;
  }

  bool initialized() const override { return initialized_; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }

  DataChunks& src_chunks() override { return source_chunks; }

  template <typename ColT, typename... Args>
  DedupCollector<ColT, Args...> getDedupCollector(const Args&... args);

  template <typename ColT, typename... Args>
  ColT* allocate(const Args&... args) {
    columns.emplace_back(std::make_shared<ColT>(local_pool_, args...));
    return dynamic_cast<ColT*>(columns.back().get());
  }
  int cur_idx_;
  int alias_;
  std::vector<std::shared_ptr<IContextColumn>> columns;

  DataChunks source_chunks;
  bool initialized_;
  std::shared_ptr<IOprState> src_state_;
  const LocalMemPool& local_pool_;
};

template <typename ColT, typename... Args>
class DedupCollector {
 public:
  DedupCollector(DedupState& state, const Args&... args)
      : state_(state), args_(args...) {
    col_ = state_.allocate<ColT, Args...>(args...);
  }
  template <typename... Params>
  void push_back(Params&&... params) {
    col_->push_back_opt(std::forward<Params>(params)...);
    if (col_->full()) {
      col_ = std::apply(
          [&](const auto&... args) {
            return state_.allocate<ColT, Args...>(args...);
          },
          args_);
    }
  }
  ColT* col_;
  DedupState& state_;
  std::tuple<Args...> args_;
};

template <typename ColT, typename... Args>
DedupCollector<ColT, Args...> DedupState::getDedupCollector(
    const Args&... args) {
  return DedupCollector<ColT, Args...>(*this, args...);
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_DEDUP_STATE_H_