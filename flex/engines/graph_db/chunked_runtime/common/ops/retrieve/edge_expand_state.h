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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_EDGE_EXPAND_STATE_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunks.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
template <typename ColT, typename... Args>
class EdgeExpandCollector;

struct LocalEdgeExpandState {
  LocalEdgeExpandState() : cur_vec_idx(0) {}
  inline void clear() { cur_vec_idx = 0; }

  template <typename ColT, typename... Args>
  EdgeExpandCollector<ColT, Args...> getEdgeCollector(const Args&... args);

  template <typename ColT, typename... Args>
  std::tuple<ColT*, ValueColumn<size_t>*, ValueColumn<size_t>*> allocate(
      const Args&... args) {
    if (cur_vec_idx >= context_columns.size()) {
      // TODO: fixme how to make a new edge column
      context_columns.emplace_back(std::make_shared<ColT>(args...));
      offsets_.emplace_back();
      leaves_offsets_.emplace_back();
      auto res = std::make_tuple(
          dynamic_cast<ColT*>(context_columns[cur_vec_idx].get()),
          &offsets_[cur_vec_idx], &leaves_offsets_[cur_vec_idx]);
      cur_vec_idx++;
      return res;
    } else {
      context_columns[cur_vec_idx]->clear();
      // dynamic_cast<ColT*>(context_columns[cur_vec_idx].get())
      //   ->init(std::forward<Args>(args)...);
      offsets_[cur_vec_idx].clear();
      leaves_offsets_[cur_vec_idx].clear();
      auto res = std::make_tuple(
          dynamic_cast<ColT*>(context_columns[cur_vec_idx].get()),
          &offsets_[cur_vec_idx], &leaves_offsets_[cur_vec_idx]);
      cur_vec_idx++;
      return res;
    }
  }

  std::vector<std::shared_ptr<IContextColumn>> context_columns;
  std::vector<ValueColumn<size_t>> offsets_;
  std::vector<ValueColumn<size_t>> leaves_offsets_;
  size_t cur_vec_idx;
};

template <typename ColT, typename... Args>
struct EdgeExpandCollector {
  ColT* edge_col;
  std::tuple<Args...> args;
  ValueColumn<size_t>* offsets;
  ValueColumn<size_t>* leaves_offsets;
  LocalEdgeExpandState& state;
  size_t previous_offset;

  EdgeExpandCollector(LocalEdgeExpandState& s, const Args&... args);
  inline void push_back_null(size_t offset) {
    if (previous_offset != offset) {
      previous_offset = offset;
      size_t sz = edge_col->size();
      offsets->push_back(offset);
      leaves_offsets->push_back((sz << 32));
    }
    auto size = leaves_offsets->size();
    (*leaves_offsets)[size - 1] += 1;
    edge_col->push_back_null();
    if (__glibc_unlikely(is_full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      std::tie(edge_col, offsets, leaves_offsets) = std::apply(
          [&](const auto&... args) {
            return state.allocate<ColT, Args...>(args...);
          },
          args);
    }
  }
  template <typename... Params>
  inline void push_back_opt(size_t offset, Params&&... params) {
    if (previous_offset != offset) {
      size_t sz = edge_col->size();
      previous_offset = offset;
      offsets->push_back(offset);
      leaves_offsets->push_back((sz << 32));
    }
    auto size = leaves_offsets->size();

    (*leaves_offsets)[size - 1] += 1;
    edge_col->push_back_opt(std::forward<Params>(params)...);
    if (__glibc_unlikely(is_full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      std::tie(edge_col, offsets, leaves_offsets) = std::apply(
          [&](const auto&... arg) {
            return state.allocate<ColT, Args...>(arg...);
          },
          args);
    }
  }

  inline bool is_full() const { return edge_col->full(); }
};

template <typename ColT, typename... Args>
EdgeExpandCollector<ColT, Args...>::EdgeExpandCollector(LocalEdgeExpandState& s,
                                                        const Args&... rargs)
    : edge_col(nullptr),
      args(rargs...),
      offsets(nullptr),
      leaves_offsets(nullptr),
      state(s),
      previous_offset(std::numeric_limits<uint32_t>::max()) {
  std::tie(edge_col, offsets, leaves_offsets) = state.allocate<ColT>(rargs...);
}

struct EdgeExpandState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {
    cur_col_ = 0;
    cur_idx_ = 0;
    source_chunks.clear();
    local_states.clear();
  }

  void initialize(int src_table, int alias) {
    cur_col_ = 0;
    cur_idx_ = 0;
    src_table_ = src_table;
    alias_ = alias;
    initialized_ = true;
  }

  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= local_states.size()) {
      return false;
    }
    bool flag = false;
    for (size_t i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= local_states.size()) {
          break;  // No more local states to process
        }
        if (cur_col_ >= local_states[cur_idx_].context_columns.size()) {
          cur_idx_++;
          cur_col_ = 0;
          continue;  // Move to the next local state
        }
        if (local_states[cur_idx_].context_columns[cur_col_]->size() == 0) {
          cur_col_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto& local_state = local_states[cur_idx_];
        auto& vertex_col = local_state.context_columns[cur_col_];
        auto& offsets = local_state.offsets_[cur_col_];
        auto& leaves_offsets = local_state.leaves_offsets_[cur_col_];
        chunks.emplace_back(DataChunk::create(source_chunks[cur_idx_], offsets,
                                              leaves_offsets, vertex_col,
                                              alias_, src_table_));
        cur_col_++;
        break;
      }
    }

    return flag;
  }

  bool initialized() const override { return initialized_; }

  EdgeExpandState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(src_state) {}

  LocalEdgeExpandState& getLocalEdgeExpandState() {
    local_states.emplace_back();
    return local_states.back();
  }

  std::vector<LocalEdgeExpandState> local_states;

  bool initialized_;
  DataChunks source_chunks;
  std::shared_ptr<IOprState> src_state_;
  size_t cur_col_, cur_idx_, src_table_, alias_;
};

struct TCCollector;
struct LocalTCState {
  LocalTCState() : cur_vec_idx(0) {}
  inline void clear() { cur_vec_idx = 0; }

  TCCollector getTCCollector(label_t label0, label_t label1);

  std::tuple<SLVertexColumn*, SLVertexColumn*, ValueColumn<size_t>*,
             ValueColumn<size_t>*>
  allocate(label_t label0, label_t label1) {
    if (cur_vec_idx >= vertex_column0.size()) {
      // TODO: fixme how to make a new edge column
      vertex_column0.emplace_back(std::make_shared<SLVertexColumn>(label0));
      vertex_column1.emplace_back(std::make_shared<SLVertexColumn>(label1));
      offsets_.emplace_back();
      leaves_offsets_.emplace_back();
      auto res = std::make_tuple(
          vertex_column0[cur_vec_idx].get(), vertex_column1[cur_vec_idx].get(),
          &offsets_[cur_vec_idx], &leaves_offsets_[cur_vec_idx]);
      cur_vec_idx++;
      return res;
    } else {
      vertex_column0[cur_vec_idx]->clear();
      // dynamic_cast<ColT*>(context_columns[cur_vec_idx].get())
      //   ->init(std::forward<Args>(args)...);
      offsets_[cur_vec_idx].clear();
      leaves_offsets_[cur_vec_idx].clear();
      vertex_column1[cur_vec_idx]->clear();
      auto res = std::make_tuple(
          vertex_column0[cur_vec_idx].get(), vertex_column1[cur_vec_idx].get(),
          &offsets_[cur_vec_idx], &leaves_offsets_[cur_vec_idx]);
      cur_vec_idx++;
      return res;
    }
  }

  std::vector<std::shared_ptr<SLVertexColumn>> vertex_column0;
  std::vector<std::shared_ptr<SLVertexColumn>> vertex_column1;
  std::vector<ValueColumn<size_t>> offsets_;
  std::vector<ValueColumn<size_t>> leaves_offsets_;
  size_t cur_vec_idx;
};

struct TCCollector {
  TCCollector(LocalTCState& s, label_t label0, label_t label1)
      : state(s), label0(label0), label1(label1) {
    std::tie(col0, col1, offsets, leaves_offsets) =
        state.allocate(label0, label1);
    previous_offset = std::numeric_limits<uint32_t>::max();
  }

  inline void push_back(size_t offset, vid_t vid0, vid_t vid1) {
    if (previous_offset != offset) {
      size_t sz = col0->size();
      offsets->push_back(offset);
      leaves_offsets->push_back((sz << 32));
      previous_offset = offset;
    }
    auto size = leaves_offsets->size();
    (*leaves_offsets)[size - 1] += 1;
    col0->push_back_opt(vid0);
    col1->push_back_opt(vid1);
    if (__glibc_unlikely(col0->full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      std::tie(col0, col1, offsets, leaves_offsets) =
          state.allocate(label0, label1);
    }
  }

  LocalTCState& state;
  label_t label0, label1;
  size_t previous_offset = std::numeric_limits<uint32_t>::max();
  SLVertexColumn* col0;
  SLVertexColumn* col1;
  ValueColumn<size_t>* offsets;
  ValueColumn<size_t>* leaves_offsets;
};

struct TCState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {
    cur_col_ = 0;
    cur_idx_ = 0;
    source_chunks.clear();
    local_states.clear();
  }

  void initialize(int src_table, int alias0, int alias1) {
    src_table_ = src_table;
    alias0_ = alias0;
    alias1_ = alias1;
    cur_idx_ = 0;
    cur_col_ = 0;
    initialized_ = true;
  }
  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= local_states.size()) {
      return false;
    }
    bool flag = false;
    for (size_t i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= local_states.size()) {
          break;  // No more local states to process
        }
        if (cur_col_ >= local_states[cur_idx_].vertex_column0.size()) {
          cur_idx_++;
          cur_col_ = 0;
          continue;  // Move to the next local state
        }
        if (local_states[cur_idx_].vertex_column0[cur_col_]->size() == 0) {
          cur_col_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto& local_state = local_states[cur_idx_];
        auto& vertex_col0 = local_state.vertex_column0[cur_col_];
        auto& vertex_col1 = local_state.vertex_column1[cur_col_];
        auto& offsets = local_state.offsets_[cur_col_];
        auto& leaves_offsets = local_state.leaves_offsets_[cur_col_];
        chunks.emplace_back(DataChunk::create(
            source_chunks[cur_idx_], offsets, leaves_offsets,
            std::vector<std::pair<std::shared_ptr<IContextColumn>, int>>{
                {vertex_col0, alias0_}, {vertex_col1, alias1_}},
            src_table_));
        cur_col_++;
        break;
      }
    }

    return flag;
  }

  bool initialized() const override { return initialized_; }

  TCState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(src_state) {}

  LocalTCState& getLocalTCState() {
    local_states.emplace_back();
    return local_states.back();
  }

  std::vector<LocalTCState> local_states;

  bool initialized_;
  DataChunks source_chunks;
  std::shared_ptr<IOprState> src_state_;
  size_t cur_col_, cur_idx_, alias0_, alias1_, src_table_;
};

template <typename ColT, typename... Args>
EdgeExpandCollector<ColT, Args...> LocalEdgeExpandState::getEdgeCollector(
    const Args&... args) {
  return EdgeExpandCollector<ColT, Args...>(*this, args...);
}
}  // namespace chunked_runtime
}  // namespace gs
#endif