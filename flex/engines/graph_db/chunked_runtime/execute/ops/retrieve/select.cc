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

#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/select.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/select_state.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
class SelectIdNeOpr : public IReadOpr {
 public:
  SelectIdNeOpr(std::unique_ptr<IReadOpr> src_opr,
                const common::Expression& expr)
      : src_opr_(std::move(src_opr)), expr_(expr) {
    tag_ = expr_.operators(0).var().tag().id();
  }

  std::string get_operator_name() const override { return "SelectIdNeOpr"; }

  IReadOpr* source_opr() const override { return src_opr_.get(); }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline,
      const LocalMemPool& mem_pool) override {
    auto ptr = src_opr_->initState(state_from_other_pipeline, mem_pool);
    return std::make_shared<SelectState>(
        ptr, expr_.operators(0).var().tag().id(), mem_pool);
  }

  bl::result<void> EvalChunks(const GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& select_state = dynamic_cast<SelectState&>(state);
    select_state.initialize();
    auto& chunks = select_state.src_chunks();

    int64_t oid = std::stoll(params.at(expr_.operators(2).param().name()));
    return ForEachChunk(chunks, [&](DataChunk& chunk) {
      auto& local_state = select_state.getLocalState();
      const auto& set = chunk.get_vertex_labels_set(tag_);
      vid_t v_ = std::numeric_limits<vid_t>::max();
      label_t label_ = std::numeric_limits<label_t>::max();
      for (auto& label : set) {
        if (!graph.GetVertexIndex(label, oid, v_)) {
          continue;
        } else {
          label_ = label;
          break;
        }
      }
      chunk.foreach_vertex(tag_, [&](size_t idx, label_t label, vid_t vid) {
        if (vid != v_ || label != label_) {
          local_state.push_back(idx);
        }
      });
    });
  }

 private:
  std::unique_ptr<IReadOpr> src_opr_;
  common::Expression expr_;
  int tag_;
};
bl::result<ReadOpBuildResultT> SelectOprBuilder::Build(
    std::unique_ptr<IReadOpr>& src_opr, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  auto opr = plan.plan(op_idx).opr().select();
  auto type = gs::runtime::parse_sp_pred(opr.predicate());
  const auto& op2 = opr.predicate().operators(2);
  if (type == gs::runtime::SPPredicateType::kPropertyNE && op2.has_param()) {
    auto var = opr.predicate().operators(0).var();
    if (var.has_property()) {
      auto name = var.property().key().name();
      auto type = gs::runtime::parse_from_ir_data_type(
          opr.predicate().operators(2).param().data_type());
      if (name == "id" && type == gs::runtime::RTAnyType::kI64Value) {
        return std::make_pair(std::make_unique<SelectIdNeOpr>(
                                  std::move(src_opr), opr.predicate()),
                              ctx_meta);
      }
    }
  }
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs