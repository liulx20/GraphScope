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
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/project.h"
#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/project_state.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
class ProjectOpr : public IReadOpr {
 public:
  ProjectOpr(std::unique_ptr<IReadOpr>&& src_opr,
             std::vector<std::pair<int, int>>&& tag_alias,
             std::vector<std::pair<std::string, RTAnyType>>&& properties_names)
      : source_opr_(std::move(src_opr)),
        tag_alias_(std::move(tag_alias)),
        properties_names_(std::move(properties_names)) {
    CHECK(source_opr_ != nullptr);
  }

  std::string get_operator_name() const override { return "ProjectOpr"; }

  IReadOpr* source_opr() const override { return source_opr_.get(); }

  std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline,
      const LocalMemPool& mem_pool) override {
    auto ptr = source_opr_->initState(state_from_other_pipeline, mem_pool);
    auto state = std::make_shared<ProjectState>(ptr, tag_alias_,
                                                properties_names_, mem_pool);
    return state;
  }

  bl::result<void> EvalChunks(const GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& project_state = dynamic_cast<ProjectState&>(state);
    project_state.initialize(&graph);
    return bl::result<void>();
  }
  std::unique_ptr<IReadOpr> source_opr_;

  std::vector<std::pair<int, int>> tag_alias_;
  std::vector<std::pair<std::string, RTAnyType>> properties_names_;
};
bool enable_project_expr(const common::Expression& expr) {
  if (expr.operators_size() != 1) {
    return false;
  }
  if (expr.operators(0).item_case() != common::ExprOpr::ItemCase::kVar) {
    return false;
  }
  const auto& var = expr.operators(0).var();
  if (!var.has_tag()) {
    return false;
  }
  if (var.has_property()) {
    if (var.property().has_len()) {
      return false;
    }
    if (var.property().has_id()) {
      return false;
    }
  }
  return true;
}

RTAnyType parse_type(const common::IrDataType& data_type) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    auto type = gs::runtime::parse_from_ir_data_type(data_type);
    return type;
  }
  case common::IrDataType::kGraphType: {
    if (data_type.graph_type().element_opt() ==
        common::GraphDataType_GraphElementOpt::
            GraphDataType_GraphElementOpt_VERTEX) {
      return gs::runtime::RTAnyType::kVertex;
    } else if (data_type.graph_type().element_opt() ==
               common::GraphDataType_GraphElementOpt::
                   GraphDataType_GraphElementOpt_EDGE) {
      return gs::runtime::RTAnyType::kEdge;
    }
  }
  default:
    LOG(FATAL) << "unrecognized data type - " << data_type.DebugString();
    break;
  }
  return gs::runtime::RTAnyType::kUnknown;
}

bl::result<ReadOpBuildResultT> ProjectOprBuilder::Build(
    std::unique_ptr<IReadOpr>& src_opr, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  if (src_opr == nullptr) {
    return std::make_pair(nullptr, ContextMeta());
  }
  const auto& opr = plan.plan(op_idx).opr().project();
  if (opr.is_append()) {
    return std::make_pair(nullptr, ContextMeta());
  }
  ContextMeta meta;
  int mapping_size = opr.mappings_size();
  std::vector<std::pair<int, int>> tag_alias;
  for (int i = 0; i < mapping_size; ++i) {
    const auto& mapping = opr.mappings(i);
    if (enable_project_expr(mapping.expr())) {
      tag_alias.emplace_back(mapping.expr().operators(0).var().tag().id(),
                             mapping.alias().value());

    } else {
      return std::make_pair(nullptr, ContextMeta());
    }
  }
  std::vector<std::pair<std::string, RTAnyType>> property_names;

  if (plan.plan(op_idx).meta_data_size() != mapping_size) {
    meta.set(plan.plan(op_idx).opr().project().mappings(0).alias().value(),
             gs::runtime::ContextColumnType::kVertex,
             gs::runtime::RTAnyType::kVertex);
    property_names.push_back({"", gs::runtime::RTAnyType::kVertex});
  } else {
    for (int i = 0; i < mapping_size; ++i) {
      std::string name = "";
      const auto& var = opr.mappings(i).expr().operators(0).var();
      if (var.has_property()) {
        if (var.has_property()) {
          if (var.property().has_key()) {
            name = var.property().key().name();
          } else if (var.property().has_label()) {
            name = "label";
          } else {
            LOG(FATAL) << "not support for " << var.property().DebugString();
          }
        } else {
          name = "";
        }
      }

      auto type = parse_type(plan.plan(op_idx).meta_data(i).type());
      if (type == gs::runtime::RTAnyType::kVertex) {
        meta.set(opr.mappings(i).alias().value(),
                 gs::runtime::ContextColumnType::kVertex, type);
      } else if (type == gs::runtime::RTAnyType::kEdge) {
        meta.set(opr.mappings(i).alias().value(),
                 gs::runtime::ContextColumnType::kEdge, type);
      } else {
        meta.set(opr.mappings(i).alias().value(),
                 gs::runtime::ContextColumnType::kValue, type);
      }
      property_names.push_back({name, type});
    }
  }
  return std::make_pair(
      std::make_unique<ProjectOpr>(std::move(src_opr), std::move(tag_alias),
                                   std::move(property_names)),
      meta);
}
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs