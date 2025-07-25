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

#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/dedup.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/dedup_state.h"

namespace gs {
namespace chunked_runtime {
namespace ops {

class DedupOpr : public IReadOpr {
 public:
  DedupOpr(std::unique_ptr<IReadOpr>&& src_opr, int key)
      : source_opr_(std::move(src_opr)), key_(key) {}
  ~DedupOpr() override = default;

  std::string get_operator_name() const override { return "Dedup"; }

  bl::result<void> EvalChunks(const GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    // Dedup logic will be implemented here
    auto& dedup_state = dynamic_cast<DedupState&>(state);
    dedup_state.initialize(key_);
    std::vector<std::shared_ptr<IContextColumn>> columns;
    do {
      auto& source_chunks = dedup_state.src_chunks();
      int chunk_num = source_chunks.chunk_num();
      for (int i = 0; i < chunk_num; ++i) {
        columns.emplace_back(source_chunks[i].get_shared(key_));
      }
      source_chunks.clear();
    } while (source_opr_
                 ->getNextChunks(graph, params, *dedup_state.src_state(),
                                 dedup_state.src_chunks())
                 .value());
    std::unordered_set<label_t> labels_set;
    for (const auto& col : columns) {
      if (col->column_type() != ContextColumnType::kVertex) {
        LOG(FATAL) << "Dedup only supports vertex column type, but got "
                   << col->column_info();
      }
      auto vertex_col = dynamic_cast<const IVertexColumn*>(col.get());
      const auto& temp = vertex_col->get_labels_set();
      labels_set.insert(temp.begin(), temp.end());
    }
    if (labels_set.size() != 1) {
      LOG(FATAL) << "Dedup only supports single label, but got "
                 << labels_set.size() << " labels";
    }
    std::vector<bool> visited(graph.GetVertexSet(*labels_set.begin()).size(),
                              false);
    auto builder = dedup_state.getDedupCollector<SLVertexColumn, label_t>(
        *labels_set.begin());
    for (const auto& col : columns) {
      auto vertex_col = dynamic_cast<const IVertexColumn*>(col.get());
      vertex_col->foreach_vertex([&](size_t, label_t label, vid_t vid) {
        if (!visited[vid]) {
          visited[vid] = true;
          builder.push_back(vid);
        }
      });
    }
    return bl::result<void>();
  }

  IReadOpr* source_opr() const override { return source_opr_.get(); }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline,
      const LocalMemPool& mem_pool) override {
    auto state = source_opr_->initState(state_from_other_pipeline, mem_pool);
    return std::make_shared<DedupState>(state, mem_pool);
  }
  std::unique_ptr<IReadOpr> source_opr_;
  int key_;  // The key to deduplicate on
};

bl::result<ReadOpBuildResultT> DedupOprBuilder::Build(
    std::unique_ptr<IReadOpr>& src_opr, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  const auto& dedup = plan.plan(op_idx).opr().dedup();
  if (dedup.keys_size() != 1) {
    return std::make_pair(nullptr, ContextMeta());
  }
  int tag = dedup.keys(0).tag().id();
  ContextMeta meta;
  meta.set(tag, gs::runtime::ContextColumnType::kVertex,
           gs::runtime::RTAnyType::kVertex);
  return std::make_pair(std::make_unique<DedupOpr>(std::move(src_opr), tag),
                        meta);  // Return a new DedupOpr with the tag as key
  // return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs