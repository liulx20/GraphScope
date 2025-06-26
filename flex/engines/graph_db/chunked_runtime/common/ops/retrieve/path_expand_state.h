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
    dst_vertex_column.emplace_back(dst_label);
    path_column.emplace_back();
    offsets.emplace_back();
    leaves_offsets.emplace_back();
  }

  inline void push_back(size_t offset, vid_t vertex,
                        std::unique_ptr<gs::runtime::PathImpl>&& path) {
    if (previous_offset != offset) {
      size_t sz = leaves_offsets[cur_vec_idx].size();
      offsets[cur_vec_idx].push_back(offset);
      leaves_offsets[cur_vec_idx].push_back((sz << 32));
    }
    auto size = leaves_offsets[cur_vec_idx].size();
    leaves_offsets[cur_vec_idx][size - 1] += 1;
    dst_vertex_column[cur_vec_idx].push_back_opt(vertex);
    path_column[cur_vec_idx].push_back_opt(std::move(path));

    if (__glibc_unlikely(path_column[cur_vec_idx].full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      cur_vec_idx++;
      if (cur_vec_idx >= dst_vertex_column.size()) {
        dst_vertex_column.emplace_back(dst_label);
        path_column.emplace_back();
        offsets.emplace_back();
        leaves_offsets.emplace_back();
      } else {
        dst_vertex_column[cur_vec_idx].clear();
        path_column[cur_vec_idx].clear();
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
  std::vector<SLVertexColumn> dst_vertex_column;
  std::vector<GeneralPathColumn> path_column;
  std::vector<ValueColumn<size_t>> offsets;
  std::vector<ValueColumn<size_t>> leaves_offsets;
};

struct PathState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {}

  bool getNextChunks(DataChunks& chunks) override { return false; }

  bool initialized() const override { return initialized_; }

  PathState(std::shared_ptr<IOprState> src_state)
      : initialized_(false), src_state_(src_state) {}

  LocalPathState& getLocalPathState() {
    local_states.emplace_back();
    return local_states.back();
  }

  std::vector<LocalPathState> local_states;

  bool initialized_;
  DataChunks source_chunks;
  std::shared_ptr<IOprState> src_state_;
};

struct LocalSSSPState {
  LocalSSSPState() = default;
  void init(label_t label) {
    dst_label = label;
    cur_vec_idx = 0;
    previous_offset = std::numeric_limits<uint32_t>::max();
    dst_vertex_column.emplace_back(std::make_shared<SLVertexColumn>(dst_label));
    len_column.emplace_back();
    offsets.emplace_back();
    leaves_offsets.emplace_back();
  }
  inline void push_back(size_t offset, vid_t vertex, int len) {
    if (previous_offset != offset) {
      size_t sz = leaves_offsets[cur_vec_idx].size();
      offsets[cur_vec_idx].push_back(offset);
      leaves_offsets[cur_vec_idx].push_back((sz << 32));
    }
    auto size = leaves_offsets[cur_vec_idx].size();
    leaves_offsets[cur_vec_idx][size - 1] += 1;
    dst_vertex_column[cur_vec_idx]->push_back_opt(vertex);
    len_column[cur_vec_idx].push_back(len);

    if (__glibc_unlikely(len_column[cur_vec_idx].full())) {
      previous_offset = std::numeric_limits<uint32_t>::max();
      cur_vec_idx++;
      if (cur_vec_idx >= dst_vertex_column.size()) {
        dst_vertex_column.emplace_back(
            std::make_shared<SLVertexColumn>(dst_label));
        len_column.emplace_back();
        offsets.emplace_back();
        leaves_offsets.emplace_back();
      } else {
        dst_vertex_column[cur_vec_idx]->clear();
        len_column[cur_vec_idx].clear();
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
  std::vector<ValueColumn<int>> len_column;
  std::vector<ValueColumn<size_t>> offsets;
  std::vector<ValueColumn<size_t>> leaves_offsets;
};

struct SSSPState : public IOprState {
  DataChunks& src_chunks() override { return source_chunks; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }
  void clear() override {}

  bool getNextChunks(DataChunks& chunks) override { return false; }

  bool initialized() const override { return initialized_; }

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
};

}  // namespace chunked_runtime
}  // namespace gs
#endif