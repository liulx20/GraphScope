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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PATH_EXPAND_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PATH_EXPAND_STATE_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunks.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/path_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
namespace gs {
namespace chunked_runtime {

struct LocalPathState {
  LocalPathState() = default;
  void init(label_t label) {
    dst_label = label;
    cur_vec_idx = 0;
    previous_offset = std::numeric_limits<uint32_t>::max();
    dst_vertex_column.emplace_back(std::make_shared<SLVertexColumn>(dst_label));
    path_column.emplace_back(std::make_shared<GeneralPathColumn>());
    offsets.emplace_back();
    leaves_offsets.emplace_back();
  }

  inline void push_back(size_t offset, vid_t vertex,
                        std::unique_ptr<gs::runtime::PathImpl>&& path) {
    if (previous_offset != offset) {
      size_t sz = path_column[cur_vec_idx]->size();
      offsets[cur_vec_idx].push_back(offset);
      leaves_offsets[cur_vec_idx].push_back((sz << 32));
      previous_offset = offset;
    }

    auto size = leaves_offsets[cur_vec_idx].size();
    leaves_offsets[cur_vec_idx][size - 1] += 1;
    dst_vertex_column[cur_vec_idx]->push_back_opt(vertex);
    path_column[cur_vec_idx]->push_back_opt(std::move(path));

    if (__glibc_unlikely(path_column[cur_vec_idx]->full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      cur_vec_idx++;
      if (cur_vec_idx >= dst_vertex_column.size()) {
        dst_vertex_column.emplace_back(
            std::make_shared<SLVertexColumn>(dst_label));
        path_column.emplace_back(std::make_shared<GeneralPathColumn>());
        offsets.emplace_back();
        leaves_offsets.emplace_back();
      } else {
        dst_vertex_column[cur_vec_idx]->clear();
        path_column[cur_vec_idx]->clear();
        offsets[cur_vec_idx].clear();
        leaves_offsets[cur_vec_idx].clear();
      }
    }
  }
  void clear() {
    cur_vec_idx = 0;
    previous_offset = std::numeric_limits<size_t>::max();
    dst_vertex_column.clear();
    path_column.clear();
    offsets.clear();
    leaves_offsets.clear();
    dst_vertex_column.emplace_back(std::make_shared<SLVertexColumn>(dst_label));
    path_column.emplace_back(std::make_shared<GeneralPathColumn>());
    offsets.emplace_back();
    leaves_offsets.emplace_back();
    init(dst_label);
  }
  label_t dst_label;
  size_t cur_vec_idx = 0;
  size_t previous_offset;
  std::vector<std::shared_ptr<SLVertexColumn>> dst_vertex_column;
  std::vector<std::shared_ptr<GeneralPathColumn>> path_column;
  std::vector<ValueColumn<size_t>> offsets;
  std::vector<ValueColumn<size_t>> leaves_offsets;
};

struct PathState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {
    cur_col_ = 0;
    cur_idx_ = 0;
    local_states.clear();
    source_chunks.clear();
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
        if (cur_col_ >= local_states[cur_idx_].dst_vertex_column.size()) {
          cur_idx_++;
          cur_col_ = 0;
          continue;  // Move to the next local state
        }
        if (local_states[cur_idx_].dst_vertex_column[cur_col_]->size() == 0) {
          cur_col_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto& local_state = local_states[cur_idx_];
        auto& vertex_col = local_state.dst_vertex_column[cur_col_];
        auto& path_col = local_state.path_column[cur_col_];
        auto& offsets = local_state.offsets[cur_col_];
        auto& leaves_offsets = local_state.leaves_offsets[cur_col_];
        chunks.emplace_back(DataChunk::create(
            source_chunks[cur_idx_], offsets, leaves_offsets,
            std::vector<std::pair<std::shared_ptr<IContextColumn>, int>>{
                {vertex_col, v_alias_}, {path_col, p_alias_}},
            src_table_));
        cur_col_++;
        break;
      }
    }

    return flag;
  }

  bool initialized() const override { return initialized_; }

  void initialize(int src_table, int v_alias, int p_alias) {
    src_table_ = src_table;
    v_alias_ = v_alias;
    p_alias_ = p_alias;
    cur_col_ = 0;
    cur_idx_ = 0;
    initialized_ = true;
    local_states.clear();
  }

  PathState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(src_state) {}

  LocalPathState& getLocalPathState() {
    local_states.emplace_back();
    return local_states.back();
  }

  std::vector<LocalPathState> local_states;

  bool initialized_;
  DataChunks source_chunks;
  size_t cur_col_, cur_idx_, p_alias_, v_alias_, src_table_;
  std::shared_ptr<IOprState> src_state_;
};

struct LocalSSSPState {
  LocalSSSPState() = default;
  void init(label_t label) {
    dst_label = label;
    cur_vec_idx = 0;
    previous_offset = std::numeric_limits<uint32_t>::max();
    dst_vertex_column.emplace_back(std::make_shared<SLVertexColumn>(dst_label));
    len_column.emplace_back(std::make_shared<ValueColumn<int>>());
    offsets.emplace_back();
    leaves_offsets.emplace_back();
  }
  inline void push_back(size_t offset, vid_t vertex, int len) {
    if (previous_offset != offset) {
      size_t sz = len_column[cur_vec_idx]->size();
      offsets[cur_vec_idx].push_back(offset);
      leaves_offsets[cur_vec_idx].push_back((sz << 32));
      previous_offset = offset;
    }
    auto size = leaves_offsets[cur_vec_idx].size();
    leaves_offsets[cur_vec_idx][size - 1] += 1;
    dst_vertex_column[cur_vec_idx]->push_back_opt(vertex);
    len_column[cur_vec_idx]->push_back(len);

    if (__glibc_unlikely(len_column[cur_vec_idx]->full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      cur_vec_idx++;
      if (cur_vec_idx >= dst_vertex_column.size()) {
        dst_vertex_column.emplace_back(
            std::make_shared<SLVertexColumn>(dst_label));
        len_column.emplace_back(std::make_shared<ValueColumn<int>>());
        offsets.emplace_back();
        leaves_offsets.emplace_back();
      } else {
        dst_vertex_column[cur_vec_idx]->clear();
        len_column[cur_vec_idx]->clear();
        offsets[cur_vec_idx].clear();
        leaves_offsets[cur_vec_idx].clear();
      }
    }
  }
  void clear() {
    cur_vec_idx = 0;
    previous_offset = std::numeric_limits<size_t>::max();
  }
  label_t dst_label;
  size_t cur_vec_idx = 0;
  size_t previous_offset;
  std::vector<std::shared_ptr<SLVertexColumn>> dst_vertex_column;
  std::vector<std::shared_ptr<ValueColumn<int>>> len_column;
  std::vector<ValueColumn<size_t>> offsets;
  std::vector<ValueColumn<size_t>> leaves_offsets;
};

struct SSSPState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {
    cur_col_ = 0;
    cur_idx_ = 0;
    local_states.clear();
    source_chunks.clear();
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
        if (cur_col_ >= local_states[cur_idx_].dst_vertex_column.size()) {
          cur_idx_++;
          cur_col_ = 0;
          continue;  // Move to the next local state
        }
        if (local_states[cur_idx_].dst_vertex_column[cur_col_]->size() == 0) {
          cur_col_++;
          continue;  // Skip empty columns
        }
        flag = true;
        auto& local_state = local_states[cur_idx_];
        auto& vertex_col = local_state.dst_vertex_column[cur_col_];
        auto& len_col = local_state.len_column[cur_col_];
        auto& offsets = local_state.offsets[cur_col_];
        auto& leaves_offsets = local_state.leaves_offsets[cur_col_];
        chunks.emplace_back(DataChunk::create(
            source_chunks[cur_idx_], offsets, leaves_offsets,
            std::vector<std::pair<std::shared_ptr<IContextColumn>, int>>{
                {vertex_col, alias_}, {len_col, len_alias_}},
            src_table_));
        cur_col_++;
        break;
      }
    }

    return flag;
  }

  bool initialized() const override { return initialized_; }

  void initialize(int src_table, int alias, int len_alias) {
    src_table_ = src_table;
    alias_ = alias;
    len_alias_ = len_alias;
    cur_col_ = 0;
    cur_idx_ = 0;
    initialized_ = true;
    // local_states.clear();
  }

  SSSPState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(src_state) {}

  LocalSSSPState& getLocalSSSPState() {
    local_states.emplace_back();
    return local_states.back();
  }

  std::vector<LocalSSSPState> local_states;

  bool initialized_;
  DataChunks source_chunks;
  std::shared_ptr<IOprState> src_state_;
  int alias_;
  int len_alias_;
  size_t cur_idx_;
  size_t cur_col_;
  int src_table_;
};

}  // namespace chunked_runtime
}  // namespace gs
#endif