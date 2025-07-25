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

#include "flex/engines/graph_db/chunked_runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/converter.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/dedup.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/edge.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/path.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/project.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/scan.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/select.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/vertex.h"

namespace gs {

namespace chunked_runtime {

void PlanParser::init() {
  register_read_operator_builder(std::make_unique<ops::ScanOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::TCOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::EdgeExpandGetVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::EdgeExpandOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::VertexOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::ProjectOprBuilder>());
  /**
  register_read_operator_builder(
      std::make_unique<ops::ProjectOrderByOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::ProjectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::OrderByOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::GroupByOprBuilder>());*/

  register_read_operator_builder(std::make_unique<ops::DedupOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SelectOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::SPOrderByLimitOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::SPOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::PathExpandVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::PathExpandOprBuilder>());

  /**
  register_read_operator_builder(std::make_unique<ops::JoinOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::IntersectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::LimitOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnfoldOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnionOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SinkOprBuilder>());*/
}

PlanParser& PlanParser::get() {
  static PlanParser parser;
  return parser;
}

void PlanParser::register_read_operator_builder(
    std::unique_ptr<IReadOperatorBuilder>&& builder) {
  auto ops = builder->GetOpKinds();
  read_op_builders_[*ops.begin()].emplace_back(ops, std::move(builder));
}
/**
void PlanParser::register_write_operator_builder(
    std::unique_ptr<IInsertOperatorBuilder>&& builder) {
  auto op = builder->GetOpKind();
  write_op_builders_[op] = std::move(builder);
}

void PlanParser::register_update_operator_builder(
    std::unique_ptr<IUpdateOperatorBuilder>&& builder) {
  auto op = builder->GetOpKind();
  update_op_builders_[op] = std::move(builder);
}*/

static std::string get_opr_name(
    physical::PhysicalOpr_Operator::OpKindCase op_kind) {
  switch (op_kind) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
    return "join";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
    return "root";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
    return "intersect";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
    return "union";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
    return "unfold";
  }
  default:
    return "unknown";
  }
}

bl::result<std::tuple<std::unique_ptr<IReadOpr>, ContextMeta, int>>
PlanParser::parse_read_pipeline_with_meta(const gs::Schema& schema,
                                          const ContextMeta& ctx_meta,
                                          const physical::PhysicalPlan& plan) {
  int opr_num = plan.plan_size();
  int i = 0;
  std::unique_ptr<IReadOpr> previous_opr = nullptr;
  ContextMeta cur_ctx_meta = ctx_meta;
  for (; i < opr_num;) {
    physical::PhysicalOpr_Operator::OpKindCase cur_op_kind =
        plan.plan(i).opr().op_kind_case();
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kSink) {
      break;
    }
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kRoot) {
      break;
    }
    auto& builders = read_op_builders_[cur_op_kind];
    int old_i = i;
    gs::Status status = gs::Status::OK();
    for (auto& pair : builders) {
      auto pattern = pair.first;
      auto& builder = pair.second;
      if (pattern.size() > static_cast<size_t>(opr_num - i)) {
        continue;
      }
      bool match = true;
      for (size_t j = 1; j < pattern.size(); ++j) {
        if (plan.plan(i + j).opr().op_kind_case() != pattern[j]) {
          match = false;
        }
      }
      if (match) {
        bl::result<ReadOpBuildResultT> res_pair_status = bl::try_handle_some(
            [&builder, &schema, &cur_ctx_meta, &plan, &i,
             &previous_opr]() -> bl::result<ReadOpBuildResultT> {
              return builder->Build(previous_opr, schema, cur_ctx_meta, plan,
                                    i);
            },
            [&status](const gs::Status& err) {
              status = err;
              return std::make_pair(nullptr, ContextMeta());
            },
            [&](const bl::error_info& err) {
              status =
                  gs::Status(gs::StatusCode::INTERNAL_ERROR,
                             "Error: " + std::to_string(err.error().value()) +
                                 ", Exception: " + err.exception()->what());
              return std::make_pair(std::unique_ptr<IReadOpr>(nullptr),
                                    ContextMeta());
            },
            [&]() {
              status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
              return std::make_pair(std::unique_ptr<IReadOpr>(nullptr),
                                    ContextMeta());
            });
        if (res_pair_status) {
          auto& opr = res_pair_status.value().first;
          auto& new_ctx_meta = res_pair_status.value().second;
          if (opr) {
            previous_opr = std::move(opr);
            cur_ctx_meta = new_ctx_meta;
            i = builder->stepping(i);
            // Reset status to OK after a successful match.
            status = gs::Status::OK();
            break;
          } else {
            continue;
          }
        }
      }
    }
    if (i == old_i) {
      break;
    } else {
      CHECK(previous_opr != nullptr)
          << "[Parse Failed] " << get_opr_name(cur_op_kind)
          << " failed to parse plan at index " << i;
    }
  }
  return std::make_tuple(std::move(previous_opr), cur_ctx_meta, i);
}

bl::result<
    std::tuple<std::unique_ptr<gs::runtime::IReadOperator>, ContextMeta, int>>
PlanParser::parse_read_pipeline(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan) {
  auto ret = parse_read_pipeline_with_meta(schema, ctx_meta, plan);
  if (!ret) {
    return ret.error();
  }
  if (std::get<0>(ret.value()) == nullptr) {
    return std::make_tuple(nullptr, std::get<1>(ret.value()), 0);
  }

  std::unique_ptr<gs::runtime::IReadOperator> opr =
      std::make_unique<ops::Converter>(std::move(std::get<0>(ret.value())),
                                       std::get<1>(ret.value()));
  return std::make_tuple(std::move(opr), std::get<1>(ret.value()),
                         std::get<2>(ret.value()));
}
/**

bl::result<InsertPipeline> PlanParser::parse_write_pipeline(
    const gs::Schema& schema, const physical::PhysicalPlan& plan) {
  std::vector<std::unique_ptr<IInsertOperator>> operators;
  for (int i = 0; i < plan.plan_size(); ++i) {
    auto op_kind = plan.plan(i).opr().op_kind_case();
    if (write_op_builders_.find(op_kind) == write_op_builders_.end()) {
      std::stringstream ss;
      ss << "[Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      //      LOG(ERROR) << err.ToString();
      return bl::new_error(err);
    }
    auto op = write_op_builders_.at(op_kind)->Build(schema, plan, i);
    if (!op) {
      std::stringstream ss;
      ss << "[Parse Failed]" << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      return bl::new_error(err);
    }
    operators.emplace_back(std::move(op));
  }
  return InsertPipeline(std::move(operators));
}

bl::result<UpdatePipeline> PlanParser::parse_update_pipeline(
    const gs::Schema& schema, const physical::PhysicalPlan& plan) {
  auto res = parse_write_pipeline(schema, plan);
  // insert pipeline
  if (res) {
    return UpdatePipeline(std::move(res.value()));
  }
  std::vector<std::unique_ptr<IUpdateOperator>> operators;
  for (int i = 0; i < plan.plan_size(); ++i) {
    auto op_kind = plan.plan(i).opr().op_kind_case();
    if (update_op_builders_.find(op_kind) == update_op_builders_.end()) {
      std::stringstream ss;
      ss << "[Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      return bl::new_error(err);
    }
    auto op = update_op_builders_.at(op_kind)->Build(schema, plan, i);
    if (!op) {
      std::stringstream ss;
      ss << "[Parse Failed]" << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      return bl::new_error(err);
    }
    operators.emplace_back(std::move(op));
  }
  return UpdatePipeline(std::move(operators));
}*/

}  // namespace chunked_runtime

}  // namespace gs
