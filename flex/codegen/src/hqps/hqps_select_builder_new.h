/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_HQPS_HQPS_SELECT_BUILDER_NEW_H_
#define CODEGEN_SRC_HQPS_HQPS_SELECT_BUILDER_NEW_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
static constexpr const char* SELECT_OP_TEMPLATE_STR =
    "%1%auto %2% = Select::select(%3%, %4%);\n";
class SelectOpBuilder {
 public:
  SelectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SelectOpBuilder& expr(const common::Expression& expr) {
    ExprBuilderNEW builder(ctx_);
    std::tie(expr_, expr_code_) = builder.AddAllExprOpr(expr.operators());
    return *this;
  }
  std::string Build() const {
    auto [ctx_prev_name, ctx_next_name] = ctx_.GetPrevAndNextCtxName();
    boost::format fmt(SELECT_OP_TEMPLATE_STR);
    fmt % expr_ % ctx_prev_name % ctx_next_name % expr_code_;
    return fmt.str();
  }

 private:
  BuildingContext& ctx_;
  std::string expr_;
  std::string expr_code_;
};
static std::string BuildSelectOp(
    BuildingContext& ctx, const algebra::Select& select_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  SelectOpBuilder builder(ctx);
  builder.expr(select_pb.predicate());

  return builder.Build();
}
}  // namespace gs
#endif