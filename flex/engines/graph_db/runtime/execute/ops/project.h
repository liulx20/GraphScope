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

#ifndef RUNTIME_EXECUTE_OPS_PROJECT_H_
#define RUNTIME_EXECUTE_OPS_PROJECT_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

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

class ProjectOprBuilder : public IReadOperatorBuilder {
 public:
  ProjectOprBuilder() = default;
  ~ProjectOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
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

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kProject};
  }
};

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

class ProjectInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  ProjectInsertOprBuilder() = default;
  ~ProjectInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const physical::PhysicalPlan& plan,
                                         int op_id) override {
    return std::make_unique<ProjectInsertOpr>(plan.plan(op_id).opr().project());
  }

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kProject;
  }
};

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

class ProjectOrderByOprBuilder : public IReadOperatorBuilder {
 public:
  ProjectOrderByOprBuilder() = default;
  ~ProjectOrderByOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
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

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kProject,
            physical::PhysicalOpr_Operator::OpKindCase::kOrderBy};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_VERTEX_H_