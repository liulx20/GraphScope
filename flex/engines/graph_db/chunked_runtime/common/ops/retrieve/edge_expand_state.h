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
      context_columns.emplace_back(std::make_unique<ColT>(args...));
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

  std::vector<std::unique_ptr<IContextColumn>> context_columns;
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
      size_t sz = leaves_offsets->size();
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
      size_t sz = leaves_offsets->size();
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
                                                        const Args&... args)
    : edge_col(nullptr),
      args(args...),
      offsets(nullptr),
      leaves_offsets(nullptr),
      state(s),
      previous_offset(std::numeric_limits<uint32_t>::max()) {
  std::tie(edge_col, offsets, leaves_offsets) = state.allocate<ColT>();
}

struct EdgeExpandState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {}

  bool getNextChunks(DataChunks& chunks) override { return false; }

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
};

}  // namespace chunked_runtime
}  // namespace gs
#endif