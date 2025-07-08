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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SCAN_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SCAN_STATE_H_
#include <limits>
#include <memory>
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
class ScanOprState : public IOprState {
 public:
  ScanOprState()
      : cur_idx_(0),
        cur_size_(0),
        cur_label_(std::numeric_limits<label_t>::max()),
        initialized_(false) {}
  void initialize(int alias) {
    initialized_ = true;
    alias_ = alias;
    }

  ~ScanOprState() override = default;

  void clear() override {
    cur_label_ = std::numeric_limits<label_t>::max();
    cur_idx_ = 0;
    cur_size_ = 0;
    vertex_columns_.clear();  // Clear the vertex columns
  }

  bool getNextChunks(DataChunks& chunks) override {
    if (cur_idx_ >= cur_size_) {
      return false;  // No more chunks to process
    }
    bool flag = false;
    for (size_t i = 0; i < Configs::MAX_THREAD_NUM; ++i) {
      while (true) {
        if (cur_idx_ >= cur_size_) {
          break;  // No more chunks to process
        }
        if (vertex_columns_[cur_idx_]->size() > 0) {
          flag = true;
          chunks.emplace_back(
              DataChunk::create(vertex_columns_[cur_idx_], alias_));
          ++cur_idx_;
          break;
          // auto& chunk = chunks[chunks.chunk_num() - 1];
          //  chunk.table_->columns_.emplace_back(vertex_columns_[cur_idx_]);
        }
        cur_idx_++;
      }
    }
    return flag;
  }

  std::shared_ptr<IOprState> src_state() override { return nullptr; }

  void append_column(label_t label) {
    if (cur_size_ >= vertex_columns_.size()) {
      vertex_columns_.emplace_back(std::make_shared<SLVertexColumn>(label));
    } else {
      vertex_columns_[cur_size_] = std::make_shared<SLVertexColumn>(label);
    }
    cur_size_++;
  }

  void start_label(label_t label) {
    if (cur_label_ != label) {
      cur_label_ = label;
      append_column(cur_label_);
    }
  }

  void collect(vid_t vid) {
    if (__glibc_unlikely(vertex_columns_[cur_size_ - 1]->full())) {
      append_column(cur_label_);
    }
    vertex_columns_[cur_size_ - 1]->push_back_opt(vid);
  }

  bool initialized() const override { return initialized_; }

  DataChunks& src_chunks() override {
    LOG(FATAL) << "ScanOprState::src_chunks() should not be called";
    static DataChunks dummy_chunks;
    return dummy_chunks;
  }

  std::vector<std::shared_ptr<SLVertexColumn>> vertex_columns_;
  size_t cur_idx_;
  size_t cur_size_;
  // The current label being processed
  label_t cur_label_;
  bool initialized_ = false;
  int alias_;
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_SCAN_STATE_H_