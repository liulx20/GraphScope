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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNKS_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNKS_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"

namespace gs {
namespace chunked_runtime {
class DataChunks {
 public:
  DataChunks() : chunk_num_(0) {}
  ~DataChunks() = default;
  void clear() {
    chunk_num_ = 0;
    for (auto& chunk : chunks_) {
      chunk.clear();
    }
  }

  void append_chunk() {
    if (chunk_num_ >= chunks_.size()) {
      chunks_.emplace_back();
    }
    chunks_[chunk_num_++].clear();
  }
  size_t chunk_num() const { return chunk_num_; }

  DataChunk& operator[](size_t idx) { return chunks_[idx]; }

  const DataChunk& operator[](size_t idx) const { return chunks_[idx]; }

 private:
  size_t chunk_num_;
  std::vector<DataChunk> chunks_;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_DATA_CHUNKS_H_