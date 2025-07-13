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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PROJECT_STATE_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PROJECT_STATE_H_
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
class ProjectState : public IOprState {
 public:
  ProjectState(
      std::shared_ptr<IOprState> src_state,
      const std::vector<std::pair<int, int>>& tag_alias,
      const std::vector<std::pair<std::string, RTAnyType>>& property_names,
      const LocalMemPool& mem_pool)
      : initialized_(false),
        tag_alias_(tag_alias),
        property_names_(property_names),

        local_pool_(mem_pool),
        src_state_(src_state),
        graph_(nullptr) {}

  void initialize(const gs::runtime::GraphReadInterface* graph) {
    initialized_ = true;
    graph_ = graph;
  }

  ~ProjectState() override = default;

  void clear() override { source_chunks.clear(); }

  bool getNextChunks(DataChunks& chunks) override {
    if (source_chunks.chunk_num() == 0) {
      return false;
    }
    size_t chunk_num = source_chunks.chunk_num();
    for (size_t i = 0; i < chunk_num; ++i) {
      const auto& src_chunk = source_chunks[i];
      DataChunk chunk;

      src_chunk.project(*graph_, chunk, tag_alias_, property_names_);
      chunks.emplace_back(std::move(chunk));
    }

    source_chunks.clear();
    return true;
  }

  std::shared_ptr<IOprState> src_state() override { return src_state_; }

  bool initialized() const override { return initialized_; }

  DataChunks& src_chunks() override { return source_chunks; }

  DataChunks source_chunks;

  bool initialized_ = false;
  std::vector<std::pair<int, int>> tag_alias_;
  std::vector<std::pair<std::string, RTAnyType>> property_names_;
  const LocalMemPool& local_pool_;
  std::shared_ptr<IOprState> src_state_;
  const gs::runtime::GraphReadInterface* graph_;
};

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_PROJECT_STATE_H_