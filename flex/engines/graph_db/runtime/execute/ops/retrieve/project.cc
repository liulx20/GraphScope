
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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/project.h"

namespace gs {
namespace runtime {
namespace ops {
class ProjectOpr : public IReadOperator {
 public:
  ProjectOpr(const physical::Project& project_opr,
             const std::vector<common::IrDataType>& data_types)
      : project_opr_(project_opr), data_types_(data_types) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_project(project_opr_, graph, std::move(ctx),
                                     params, data_types_);
  }

 private:
  physical::Project project_opr_;
  std::vector<common::IrDataType> data_types_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> ProjectOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
    }
  }
  ContextMeta ret_meta;
  if (plan.plan(op_idx).opr().project().is_append()) {
    ret_meta = ctx_meta;
  }
  for (int i = 0; i < mappings_size; ++i) {
    auto& m = plan.plan(op_idx).opr().project().mappings(i);
    int alias = -1;
    if (m.has_alias()) {
      alias = m.alias().value();
    }
    ret_meta.set(alias);
  }
  return std::make_pair(std::make_unique<ProjectOpr>(
                            plan.plan(op_idx).opr().project(), data_types),
                        ret_meta);
}

class ProjectOrderByOpr : public IReadOperator {
 public:
  ProjectOrderByOpr(const physical::Project& project_opr,
                    const algebra::OrderBy& order_by_opr,
                    const std::vector<common::IrDataType>& data_types)
      : project_opr_(project_opr),
        order_by_opr_(order_by_opr),
        data_types_(data_types) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_project_order_by(project_opr_, order_by_opr_,
                                              graph, std::move(ctx), timer,
                                              params, data_types_);
  }

 private:
  physical::Project project_opr_;
  algebra::OrderBy order_by_opr_;
  std::vector<common::IrDataType> data_types_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta>
ProjectOrderByOprBuilder::Build(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan,
                                int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
    }
  }
  if (project_order_by_fusable_beta(plan.plan(op_idx).opr().project(),
                                    plan.plan(op_idx + 1).opr().order_by(),
                                    ctx_meta, data_types)) {
    ContextMeta ret_meta;
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      ret_meta.set(alias);
    }
    return std::make_pair(
        std::make_unique<ProjectOrderByOpr>(
            plan.plan(op_idx).opr().project(),
            plan.plan(op_idx + 1).opr().order_by(), data_types),
        ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs