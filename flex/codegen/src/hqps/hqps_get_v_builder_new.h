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
#ifndef CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_NEW_H_
#define CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_NEW_H_
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
static std::string vopt_pb_to_string(const physical::GetV::VOpt& v_opt) {
  switch (v_opt) {
  case physical::GetV_VOpt_START:
    return "VOpt::kStart";
  case physical::GetV_VOpt_END:
    return "VOpt::kEnd";
  case physical::GetV_VOpt_OTHER:
    return "VOpt::kOther";
  case physical::GetV_VOpt_BOTH:
    return "VOpt::kBoth";
  case physical::GetV_VOpt_ITSELF:
    return "VOpt::kItself";
  default:
    throw std::runtime_error("unknown vopt");
  }
}

template <typename LabelT>
class GetVOptBuilder {
 public:
  GetVOptBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  GetVOpBuilder& v_opt(const physical::GetV::VOpt& v_opt) {
    v_opt_ = vopt_pb_to_string(v_opt);
    return *this;
  }

  GetVOpBuilder& in_tag(int32_t in_tag_id) {
    in_tag_id_ = ctx_.GetTagInd(in_tag_id);
    return *this;
  }

  GetVOpBuilder& out_tag(int32_t out_tag_id) {
    out_tag_id_ = out_tag_id;
    return *this;
  }

  GetVOpBuilder& add_vertex_label(const common::NameOrId& vertex_label) {
    vertex_labels_.push_back(
        try_get_label_from_name_or_id<LabelT>(vertex_label));
    return *this;
  }

  GetVOptBuilder& filter(const common::Expression& expr) {
    ExprBuilderNEW expr_builder(ctx_);
    std::tie(expr_, expr_code_) = expr_builder.Build(expr);
    return *this;
  }

  std::string Build() const {
    auto [prev_ctx_name, next_ctx_name] = ctx_.GetPrevAndNextContextName();
    static constexpr const char* GET_VERTEX_FROM_EDGES_TEMPLATE_STR =
        "%1%auto %2% = get_vertex_from_edges(txn, %3%, %4%, %5%);\n";
    static constexpr const char* GET_VERTEX_FROM_VERTEICES_TEMPLATE_STR =
        "%1%auto %2% = get_vertex_from_vertices(txn, %3%, %4%, %5%);\n";
    static constexpr const char* GET_VERTEX_PARAMS_TEMPLATE_STR =
        "GetVertexParams{%1%,%2%,%3%,%4%}";
    boost::format params_formater(GET_VERTEX_PARAMS_TEMPLATE_STR);
    std::stringstream ss{};
    ss << "{";

    for (size_t idx = 0; idx < vertex_labels_.size(); ++idx) {
      ss << std::to_string(label);
      if (idx != vertex_labels_.size() - 1) {
        ss << ",";
      } else {
        ss << "}"
      }
    }
    std::string params_str = ss.str();
    params_format % v_opt_ % in_tag_id_ % out_tag_id_ % params_str;
    params_str = params_format.str();
    if (v_opt_ == "VOpt::kStart" || v_opt_ == "VOpt::kEnd" ||
        v_opt_ == "VOpt::kOther" || v_opt_ == "VOpt::kBoth") {
      return str(boost::format(GET_VERTEX_FROM_EDGES_TEMPLATE_STR) % expr_ %
                 prev_ctx_name % next_ctx_name % params_str % expr_code_);
    } else if (v_opt_ == "VOpt::kItself") {
      return str(boost::format(GET_VERTEX_FROM_VERTEICES_TEMPLATE_STR) % expr %
                 prev_ctx_name % next_ctx_name % params_str % expr_code_);
    }
  }

 private:
  BuildingContext& ctx_;
  std::string v_opt_;
  int32_t in_tag_id_;
  int32_t out_tag_id_;
  std::vector<LabelT> vertex_labels_;
  std::string expr_code_;
  stdL::string expr_;
};

template <typename LabelT>
static std::string BuildGetVOp(
    BuildingContext& ctx, const physical::GetV& get_v_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GetVOptBuilder<LabelT> get_v_opt_builder(ctx);
  get_v_opt_builder.v_opt(get_v_pb.v_opt());
  if (get_v_pb.has_in_tag_id()) {
    get_v_opt_builder.in_tag(get_v_pb.in_tag_id());
  } else {
    get_v_opt_builder.in_tag(-1);
  }

  if (get_v_pb.has_alias()) {
    get_v_opt_builder.out_tag(get_v_pb.alias().value());
  } else {
    get_v_opt_builder.out_tag(-1);
  }

  auto& vertex_labels_pb = get_v_pb.params().tables();
  for (const auto& vertex_label : vertex_labels_pb) {
    get_v_opt_builder.add_vertex_label(vertex_label);
  }

  return get_v_opt_builder.Build();
}
}  // namespace gs
#endif  // CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_NEW_H_