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
#ifndef CODEGEN_SRC_HQPS_HQPS_GROUP_BY_BUILDER_NEW_H_
#define CODEGEN_SRC_HQPS_HQPS_GROUP_BY_BUILDER_NEW_H_

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
class GroupByOpBuilder {
 public:
  GroupByOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

 private:
  BuildingContext& ctx_;
};

static std::string BuildGroupByOp(
    BuildingContext& ctx, const physical::GroupBy& group_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GroupByOpBuilder builder(ctx);
  auto& key_alias = group_by_pb.mappings();

  return "";
}
}  // namespace gs
#endif
