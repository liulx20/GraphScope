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

#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/vertex.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/get_v.h"
#include "flex/engines/graph_db/chunked_runtime/utils/predicates.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace chunked_runtime {
namespace ops {

class GetVFromVerticesWithLabelWithInOpr : public IReadOpr {
 public:
  GetVFromVerticesWithLabelWithInOpr(std::unique_ptr<IReadOpr>&& src_opr,
                                     const physical::GetV& opr,
                                     const GetVParams& p,
                                     const std::set<label_t>& labels)
      : opr_(opr), v_params_(p), labels_set_(labels) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithLabelWithInOpr";
  }

  IReadOpr* source_opr() const override { return source_opr_.get(); }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline) override {
    auto ptr = source_opr_->initState(state_from_other_pipeline);
    return std::make_shared<GetVState>(ptr);
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    bool flag = true;
    auto& casted_state = dynamic_cast<GetVState&>(state);
    auto& chunks = casted_state.src_chunks();
    return ForEachChunk(chunks, [&](const DataChunk& chunk) {
      auto labels = chunk.get_vertex_labels_set(v_params_.tag);
      for (auto label : labels) {
        if (labels_set_.find(label) == labels_set_.end()) {
          flag = false;
          break;
        }
      }
      if (v_params_.tag == -1 && flag) {
        // TODO: fix this
        return bl::result<void>();
      } else {
        Arena arena;
        GeneralVertexPredicate pred(graph, chunk, params,
                                    opr_.params().predicate());
        auto& local_state = casted_state.getLocalState();
        return GetV::get_vertex_from_vertices(
            graph, chunk, v_params_,
            [&arena, &pred](label_t label, vid_t v) {
              return pred(label, v, arena);
            },
            local_state);
      }
    });
  }

 private:
  std::unique_ptr<IReadOpr> source_opr_;
  physical::GetV opr_;
  GetVParams v_params_;
  std::set<label_t> labels_set_;
};

class GetVFromVerticesWithPKExactOpr : public IReadOpr {
 public:
  GetVFromVerticesWithPKExactOpr(std::unique_ptr<IReadOpr>&& src_opr,
                                 const physical::GetV& opr, const GetVParams& p,
                                 label_t exact_pk_label,
                                 const std::string& exact_pk)
      : source_opr_(std::move(src_opr)),
        opr_(opr),
        v_params_(p),
        exact_pk_label_(exact_pk_label),
        exact_pk_(exact_pk) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithPKExact";
  }

  IReadOpr* source_opr() const override { return source_opr_.get(); }
  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline) override {
    auto ptr = source_opr_->initState(state_from_other_pipeline);
    return std::make_shared<GetVState>(ptr);
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<GetVState&>(state);
    auto& chunks = casted_state.src_chunks();
    return ForEachChunk(chunks, [&](const DataChunk& chunk) {
      auto& local_state = casted_state.getLocalState();
      int64_t pk = std::stoll(params.at(exact_pk_));
      vid_t index = std::numeric_limits<vid_t>::max();
      graph.GetVertexIndex(exact_pk_label_, pk, index);
      ExactVertexPredicate pred(exact_pk_label_, index);
      return GetV::get_vertex_from_vertices(graph, chunk, v_params_, pred,
                                            local_state);
    });
  }

 private:
  std::unique_ptr<IReadOpr> source_opr_;
  physical::GetV opr_;
  GetVParams v_params_;
  label_t exact_pk_label_;
  std::string exact_pk_;
};

class GetVFromVerticesWithPredicateOpr : public IReadOpr {
 public:
  GetVFromVerticesWithPredicateOpr(std::unique_ptr<IReadOpr>&& src_opr,
                                   const physical::GetV& opr,
                                   const GetVParams& p)
      : source_opr_(std::move(src_opr)), opr_(opr), v_params_(p) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithPredicate";
  }

  IReadOpr* source_opr() const override { return source_opr_.get(); }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline) override {
    auto ptr = source_opr_->initState(state_from_other_pipeline);
    return std::make_shared<GetVState>(ptr);
  }
  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<GetVState&>(state);
    auto& chunks = casted_state.src_chunks();
    return ForEachChunk(chunks, [&](DataChunk& chunk) {
      GeneralVertexPredicate pred(graph, chunk, params,
                                  opr_.params().predicate());
      Arena arena;
      auto& local_state = casted_state.getLocalState();
      return GetV::get_vertex_from_vertices(
          graph, chunk, v_params_,
          [&arena, &pred](label_t label, vid_t v) {
            return pred(label, v, arena);
          },
          local_state);
    });
  }

 private:
  std::unique_ptr<IReadOpr> source_opr_;
  physical::GetV opr_;
  GetVParams v_params_;
};

struct GeneralVertexPredicateWrapper {
  GeneralVertexPredicateWrapper(const GeneralVertexPredicate& pred)
      : pred_(pred) {}

  inline bool operator()(label_t label, vid_t v, int) const {
    return pred_(label, v, arena_, 0);
  }

  inline bool operator()(label_t label, vid_t v) const {
    return pred_(label, v, arena_);
  }
  mutable Arena arena_;

  const GeneralVertexPredicate& pred_;
};
class GetVFromEdgesWithPredicateOpr : public IReadOpr {
 public:
  GetVFromEdgesWithPredicateOpr(std::unique_ptr<IReadOpr>&& src_opr,
                                const physical::GetV& opr, const GetVParams& p)
      : source_opr_(std::move(src_opr)), opr_(opr), v_params_(p) {}

  std::string get_operator_name() const override {
    return "GetVFromEdgesWithPredicate";
  }

  IReadOpr* source_opr() const override { return source_opr_.get(); }
  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline) override {
    auto ptr = source_opr_->initState(state_from_other_pipeline);
    return std::make_shared<GetVState>(ptr);
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<GetVState&>(state);
    auto& chunks = casted_state.src_chunks();
    return ForEachChunk(chunks, [&](DataChunk& chunk) {
      auto& local_state = casted_state.getLocalState();
      if (opr_.params().has_predicate()) {
        GeneralVertexPredicate pred(graph, chunk, params,
                                    opr_.params().predicate());
        GeneralVertexPredicateWrapper vpred(pred);
        return GetV::get_vertex_from_edges(graph, chunk, v_params_, vpred,
                                           local_state);
      } else {
        return GetV::get_vertex_from_edges(graph, chunk, v_params_,
                                           DummyVertexPredicate(), local_state);
      }
    });
  }

 private:
  std::unique_ptr<IReadOpr> source_opr_;
  physical::GetV opr_;
  GetVParams v_params_;
};

bl::result<ReadOpBuildResultT> VertexOprBuilder::Build(
    std::unique_ptr<IReadOpr> src_opr, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  const auto& vertex = plan.plan(op_idx).opr().vertex();

  int alias = -1;
  if (vertex.has_alias()) {
    alias = plan.plan(op_idx).opr().vertex().alias().value();
  }

  ContextMeta ret_meta = ctx_meta;
  ret_meta.set(alias);

  int tag = -1;
  if (vertex.has_tag()) {
    tag = vertex.tag().value();
  }
  VOpt opt = gs::runtime::parse_opt(vertex.opt());

  if (!vertex.has_params()) {
    LOG(ERROR) << "GetV should have params" << vertex.DebugString();
    return std::make_pair(nullptr, ContextMeta());
  }
  GetVParams p;
  p.opt = opt;
  p.tag = tag;
  p.tables = gs::runtime::parse_tables(vertex.params());
  p.alias = alias;

  if (vertex.params().has_predicate()) {
    if (opt == VOpt::kItself) {
      // label within predicate
      {
        std::set<label_t> labels_set;
        if (gs::runtime::is_label_within_predicate(vertex.params().predicate(),
                                                   labels_set)) {
          return std::make_pair(
              std::make_unique<GetVFromVerticesWithLabelWithInOpr>(
                  std::move(src_opr), plan.plan(op_idx).opr().vertex(), p,
                  labels_set),
              ctx_meta);
        }
      }

      // pk exact check
      {
        label_t exact_pk_label;
        std::string exact_pk;
        if (gs::runtime::is_pk_exact_check(schema, vertex.params().predicate(),
                                           exact_pk_label, exact_pk)) {
          return std::make_pair(
              std::make_unique<GetVFromVerticesWithPKExactOpr>(
                  std::move(src_opr), plan.plan(op_idx).opr().vertex(), p,
                  exact_pk_label, exact_pk),
              ctx_meta);
        }
      }
      // general predicate
      return std::make_pair(
          std::make_unique<GetVFromVerticesWithPredicateOpr>(
              std::move(src_opr), plan.plan(op_idx).opr().vertex(), p),
          ctx_meta);
    } else if (opt == VOpt::kEnd || opt == VOpt::kStart) {
      return std::make_pair(
          std::make_unique<GetVFromEdgesWithPredicateOpr>(
              std::move(src_opr), plan.plan(op_idx).opr().vertex(), p),
          ctx_meta);
    }
  } else {
    if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
      return std::make_pair(
          std::make_unique<GetVFromEdgesWithPredicateOpr>(
              std::move(src_opr), plan.plan(op_idx).opr().vertex(), p),
          ctx_meta);
    }
  }

  LOG(ERROR) << "not support" << vertex.DebugString();
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs