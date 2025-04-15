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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/execute/pipeline.h"

#include "bthread/bthread.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/lambda_wrapper.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
namespace gs {
namespace runtime {
namespace ops {
class IntersectOpr : public IReadOperator {
 public:
  IntersectOpr(const physical::Intersect& intersect_opr,
               std::vector<ReadPipeline>&& sub_plans)
      : key_(intersect_opr.key()), sub_plans_(std::move(sub_plans)) {}

  std::string get_operator_name() const override { return "IntersectOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::vector<gs::runtime::Context> ctxs(sub_plans_.size());
    std::vector<bl::result<gs::runtime::Context>> ctxs_res(sub_plans_.size());
    std::vector<std::unique_ptr<LambdaWrapperBase>> wrappers(sub_plans_.size());
    std::vector<bthread_t> bths(sub_plans_.size());
    for (size_t idx = 0; idx < sub_plans_.size(); ++idx) {
      auto& plan = sub_plans_[idx];
      Context n_ctx(ctx);
      n_ctx.gen_offset();
      auto lambda = [&]() {
        ctxs_res[idx] =
            std::move(plan.Execute(graph, std::move(n_ctx), params, timer));
      };
      wrappers[idx] =
          std::make_unique<LambdaWrapper<decltype(lambda)>>(std::move(lambda));
      if (bthread_start_background(&bths[idx], NULL, LambdaExecutor,
                                   static_cast<void*>(wrappers[idx].get())) !=
          0) {
        return bl::new_error(
            gs::Status(gs::StatusCode::INTERNAL_ERROR,
                       "Failed to start thread for sub plan execution"));
      }
    }
    for (size_t idx = 0; idx < sub_plans_.size(); ++idx) {
      if (bthread_join(bths[idx], NULL) != 0) {
        return bl::new_error(
            gs::Status(gs::StatusCode::INTERNAL_ERROR,
                       "Failed to join thread for sub plan execution"));
      }
      if (!ctxs_res[idx]) {
        return bl::new_error(ctxs_res[idx].error());
      }
      ctxs[idx] = std::move(ctxs_res[idx].value());
    }

    return Intersect::intersect(std::move(ctx), std::move(ctxs), key_);
  }

 private:
  int key_;
  std::vector<ReadPipeline> sub_plans_;
};

bl::result<ReadOpBuildResultT> IntersectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<ReadPipeline> sub_plans;
  for (int i = 0; i < plan.plan(op_idx).opr().intersect().sub_plans_size();
       ++i) {
    auto& sub_plan = plan.plan(op_idx).opr().intersect().sub_plans(i);
    auto sub_plan_res = PlanParser::get().parse_read_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!sub_plan_res) {
      return std::make_pair(nullptr, ContextMeta());
    }
    sub_plans.push_back(std::move(sub_plan_res.value().first));
  }
  ContextMeta meta = ctx_meta;
  meta.set(plan.plan(op_idx).opr().intersect().key());
  return std::make_pair(
      std::make_unique<IntersectOpr>(plan.plan(op_idx).opr().intersect(),
                                     std::move(sub_plans)),
      meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs