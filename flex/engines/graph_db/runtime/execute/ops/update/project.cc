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
#include "flex/engines/graph_db/runtime/execute/ops/update/project.h"

namespace gs {
namespace runtime {
namespace ops {
class ProjectInsertOpr : public IInsertOperator {
 public:
  ProjectInsertOpr(const physical::Project& project_opr)
      : project_opr_(project_opr) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_project(project_opr_, graph, std::move(ctx),
                                     params);
  }

 private:
  physical::Project project_opr_;
};

std::unique_ptr<IInsertOperator> ProjectInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  return std::make_unique<ProjectInsertOpr>(plan.plan(op_id).opr().project());
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs