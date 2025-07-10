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

#ifndef CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_CONVERTER_H_
#define CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_CONVERTER_H_
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
#include "flex/engines/graph_db/runtime/execute/operator.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
struct ConverterState : public IOprState {
  ConverterState(std::shared_ptr<IOprState> src_state,
                 const LocalMemPool& mem_pool)
      : initialized_(false), src_state_(src_state) {}
  void clear() override { src_chunks_.clear(); }
  bool initialized() const override { return initialized_; }
  std::shared_ptr<IOprState> src_state() override { return src_state_; }

  bool getNextChunks(DataChunks& chunks) override { return false; }

  DataChunks& src_chunks() override { return src_chunks_; }

  DataChunks src_chunks_;
  bool initialized_;
  std::shared_ptr<IOprState> src_state_;
};
class Converter : public gs::runtime::IReadOperator {
 public:
  Converter(std::unique_ptr<IReadOpr>&& src,
            const gs::runtime::ContextMeta& meta)
      : source_opr_(std::move(src)), ctx_meta_(meta) {}

  ~Converter() override = default;
  std::string get_operator_name() const override { return "Converter"; }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline,
      const LocalMemPool& mem_pool) {
    auto src = source_opr_->initState(state_from_other_pipeline, mem_pool);
    return std::make_shared<ConverterState>(src, mem_pool);
  }

  void build_empty_context(gs::runtime::Context& ctx);

  void build_context(const std::vector<DataChunk>& chunks,
                     gs::runtime::Context& ctx);

  bool exec(const GraphReadInterface& graph,
            const std::map<std::string, std::string>& params,
            gs::runtime::Context& ctx) {
    std::vector<DataChunk> chunks;
    std::shared_ptr<IOprState> state =
        initState(nullptr, graph.GetLocalMemPool());
    state->clear();
    while (source_opr_
               ->getNextChunks(graph, params, *state->src_state(),
                               state->src_chunks())
               .value()) {
      const auto& src_chunks = state->src_chunks();
      for (size_t i = 0; i < src_chunks.chunk_num(); ++i) {
        chunks.emplace_back(std::move(src_chunks[i]));
      }
      state->clear();
    }

    build_context(chunks, ctx);
    return false;
  }
  bl::result<gs::runtime::Context> Eval(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    exec(graph, params, ctx);
    return ctx;
  }

  std::unique_ptr<IReadOpr> source_opr_;
  std::vector<int> alias_;
  gs::runtime::ContextMeta ctx_meta_;
};
}  // namespace ops
}  // namespace chunked_runtime

}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_OPS_RETRIEVE_CONVERTER_H_