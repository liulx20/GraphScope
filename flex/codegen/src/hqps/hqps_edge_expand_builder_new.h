#include <sstream>
#include <string>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/hqps/hqps_edge_expand_builder_new.h"
#include "flex/codegen/src/pb_parser/internal_struct.h"
#include "flex/codegen/src/pb_parser/ir_data_type_parser.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"

namespace gs {

static constexpr const char* EDGE_EXPAND_PARAMS_TEMPLATE_STR =
    "EdgeExpandParams { %1%, %2%, %3%, %4%}";
static constexpr const char* EDGE_EXPAND_E_WITH_FILTER_TEMPLATE_STR =
    "%1%auto %2% = expand_edge(txn, std::move(%3%), %4%, %5%);\n";
static constexpr const char* EDGE_EXPAND_E_TEMPLATE_STR =
    "auto %1% = expand_edge_without_predicate(txn, std::move(%2%), %3%);\n";

static constexpr const char* EDGE_EXPAND_V_WITH_FILTER_TEMPLATE_STR =
    "%1%auto %2% = expand_vertex(txn, std::move(%3%), %4%, %5%);\n";
static constexpr const char* EDGE_EXPAND_V_TEMPLATE_STR =
    "auto %1% = expand_vertex_without_predicate(txn, std::move(%2%), %3%);\n";

template <typename LabelT>
class EdgeExpandOpBuilder {
 public:
  EdgeExpandOpBuilder(BuildingContext& ctx)
      : ctx_(ctx), direction_(internal::Direction::kNotSet) {}
  ~EdgeExpandOpBuilder() = default;

  EdgeExpandOpBuilder& dstVertexLabels(
      const std::vector<LabelT>& dst_vertex_labels) {
    get_v_vertex_labels_ = dst_vertex_labels;
    return *this;
  }
  EdgeExpandOpBuilder& query_params(const algebra::QueryParams& query_params) {
    query_params_ = query_params;
    return *this;
  }

  EdgeExpandOpBuilder& expand_opt(const physical::EdgeExpand::ExpandOpt& opt) {
    expand_opt_ = opt;
    return *this;
  }

  EdgeExpandOpBuilder& direction(const physical::EdgeExpand::Direction& dir) {
    switch (dir) {
    case physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT:
      direction_ = internal::Direction::kOut;
      break;

    case physical::EdgeExpand::Direction::EdgeExpand_Direction_IN:
      direction_ = internal::Direction::kIn;
      break;

    case physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH:
      direction_ = internal::Direction::kBoth;
      break;
    default:
      LOG(FATAL) << "Unknown direction";
    }
    return *this;
  }

  EdgeExpandOpBuilder& v_tag(const int32_t& v_tag) {
    v_tag_ = ctx_.GetTagInd(v_tag);
    return *this;
  }

  EdgeExpandOpBuilder& meta_data(
      const physical::PhysicalOpr::MetaData& meta_data) {
    meta_data_ = meta_data;
    // we can get the edge tuplet from meta_data, in case we fail to extract
    // edge triplet from ir_data_type
    {
      auto& ir_data_type = meta_data_.type();
      VLOG(10) << "str: " << ir_data_type.DebugString();
      CHECK(ir_data_type.has_graph_type());
      auto& graph_ele_type = ir_data_type.graph_type();
      VLOG(10) << "debug string: " << graph_ele_type.DebugString();
      CHECK(graph_ele_type.element_opt() ==
                common::GraphDataType::GraphElementOpt::
                    GraphDataType_GraphElementOpt_EDGE ||
            graph_ele_type.element_opt() ==
                common::GraphDataType::GraphElementOpt::
                    GraphDataType_GraphElementOpt_VERTEX)
          << "expect edge meta for edge builder";
      auto& graph_data_type = graph_ele_type.graph_data_type();
      CHECK(graph_data_type.size() > 0);

      CHECK(direction_ != internal::Direction::kNotSet);

      for (auto ele_label_type : graph_data_type) {
        auto& triplet = ele_label_type.label();
        auto& dst_label = triplet.dst_label();
        edge_triples_.emplace_back(triplet.src_label(), triplet.label(),
                                   triplet.dst_label());
        edge_labels_.emplace_back(triplet.label());
        if (direction_ == internal::Direction::kOut) {
          VLOG(10) << "got dst_label : " << dst_label.value();
          dst_vertex_labels_.emplace_back(dst_label.value());
        } else if (direction_ == internal::Direction::kIn) {
          dst_vertex_labels_.emplace_back(triplet.src_label().value());
        } else {  // kBoth
          dst_vertex_labels_.emplace_back(triplet.src_label().value());
          dst_vertex_labels_.emplace_back(dst_label.value());
        }
      }
      VLOG(10) << "before join: " << gs::to_string(dst_vertex_labels_);
      VLOG(10) << "before join get_v: " << gs::to_string(get_v_vertex_labels_);
      // only interset if get_v_vertex_labels specify any labels
      if (get_v_vertex_labels_.size() > 0) {
        intersection(dst_vertex_labels_, get_v_vertex_labels_);
      }
      {
        std::unordered_set<LabelT> s(dst_vertex_labels_.begin(),
                                     dst_vertex_labels_.end());
        dst_vertex_labels_.assign(s.begin(), s.end());
        std::sort(edge_triples_.begin(), edge_triples_.end());
        size_t len = std::unique(edge_triples_.begin(), edge_triples_.end()) -
                     edge_triples_.begin();
        edge_triples_.resize(len);
      }
      VLOG(10) << "after join " << gs::to_string(dst_vertex_labels_);
      VLOG(10) << "extract dst vertex label: "
               << gs::to_string(dst_vertex_labels_) << ", from meta data";
    }
    return *this;
  }

  std::string Build() const {
    std::string opt_name, opt_code;
    // if edge expand contains only one edge_triplet, generate the simple
    // EdgeExpandOpt.
    std::unordered_set<LabelT> edge_label_set(edge_labels_.begin(),
                                              edge_labels_.end());
    if (edge_label_set.size() == 1 && dst_vertex_labels_.size() == 1) {
      LOG(INFO) << "Building simple edge expand opt, with only one edge label";
      std::tie(opt_name, opt_code) = BuildOneLabelEdgeExpandOpt(
          ctx_, direction_, query_params_, dst_vertex_labels_, expand_opt_,
          meta_data_);
    } else {
      std::tie(opt_name, opt_code) = BuildMultiLabelEdgeExpandOpt(
          ctx_, direction_, query_params_, expand_opt_, meta_data_);
    }

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    boost::format formater("");
    if (expand_opt_ ==
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      formater = boost::format(EDGE_EXPANDE_OP_TEMPLATE_STR);
    } else {
      formater = boost::format(EDGE_EXPANDV_OP_TEMPLATE_STR);
    }

    auto append_opt = res_alias_to_append_opt(res_alias_);
    formater % next_ctx_name % append_opt % format_input_col(v_tag_) %
        ctx_.GraphVar() % make_move(prev_ctx_name) % make_move(opt_name);

    return opt_code + formater.str();
  }

 private:
  static std::string convert_direction_to_string(internal::Direction dir) {
    switch (dir) {
    case internal::Direction::kOut:
      return "Direction::kOut";
    case internal::Direction::kIn:
      return "Direction::kIn";
    case internal::Direction::kBoth:
      return "Direction::kBoth";
    default:
      LOG(FATAL) << "Unknown direction";
    }
  }

  static std::pair<std::string, std::string> BuildExprFromPredicate(
      BuildingContext& ctx, const common::Expression& predicate) {
    // TODO
    ExprBuilderNEW builder(ctx);
    return builder.AddAllExprOpr(expr.operators());
  }

  template <typename LabelT>
  std::string BuildOneLabelEdgeExpandOpt() {
    std::string expr{}, expr_code{};
    if (query_params_.has_predicate()) {
      // TODO support predicate
      std::tie(expr, expr_code) =
          BuildExprFromPredicate(ctx_, query_params_.predicate());
    }

    LabelT edge_label =
        try_get_label_from_name_or_id<LabelT>(query_params_.tables()[0]);
    std::string edge_label_id_str = ensure_label_id(edge_label);

    std::string dst_label_ids_str{};
    if (dst_vertex_labels_.size() == 1) {
      dst_label_ids_str = ensure_label_id();
    } else {
      // TODO not support here
    }

    /**
    struct EdgeExpandParams {
    int v_tag;
    std::vector<LabelTriplet> labels;
    int alias;
    Direction dir;
  };
  expand_edge_without_predicate(txn, ctx, params);
  */
    auto [prev_ctx, next_ctx] = ctx_.GetPrevAndNextCtxName();

    std::string dir_str = convert_direction_to_string(direction_);
    std::string edge_expand_params_str{};
    {
      boost::format formater(EDGE_EXPAND_E_TEMPLATE_STR);
      stringstream ss{};
      ss << "{";
      for (size_t i = 0; i < edge_triples_.size(); ++i) {
        auto& triplet = edge_triples_[i];
        ss << "{";
        ss << ensure_label_id(std::get<0>(triplet)) << ", ";
        ss << ensure_label_id(std::get<1>(triplet)) << ", ";
        ss << ensure_label_id(std::get<2>(triplet));
        if (i + 1 == edge_triples_.size()) {
          ss << "}";
        } else {
          ss << "}, ";
        }
      }
      ss << "}";
      formater % std::to_string(v_tag_) % ss.str() %
          std::to_string(res_alias_) % dir_str;
      edge_expand_params_str = formater.str();
    }

    if (expand_opt_ ==
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      if (query_params_.has_predicate()) {
        // TODO support predicate
        boost::format formater(EDGE_EXPAND_E_WITH_FILTER_TEMPLATE_STR);
        formater % expr % next_ctx % prev_ctx % edge_label_id_str %
            edge_expand_params_str % expr_code;
        return formater.str();

      } else {
        boost::format formater(EDGE_EXPAND_E_TEMPLATE_STR);
        formater % next_ctx % prev_ctx % edge_expand_params_str;
        return formater.str();
      }
    } else if (expand_opt_ ==
               physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      if (query_params_.has_predicate()) {
        boost::format formater(EDGE_EXPAND_V_WITH_FILTER_TEMPLATE_STR);
        formater % expr % next_ctx % prev_ctx % dst_label_ids_str %
            edge_expand_params_str % expr_code;
        return formater.str();
      } else {
        boost::format formater(EDGE_EXPAND_V_TEMPLATE_STR);
        formater % next_ctx % prev_ctx % edge_expand_params_str;
        return formater.str();
      }
    }
  }

 private:
  BuildingContext& ctx_;
  int32_t res_alias_;
  algebra::QueryParams query_params_;
  physical::EdgeExpand::ExpandOpt expand_opt_;
  internal::Direction direction_;
  std::vector<LabelT> dst_vertex_labels_;
  std::vector<LabelT> edge_labels_;
  std::vector<LabelT> get_v_vertex_labels_;
  std::vector<std::tuple<LabelT, LabelT, LabelT>> edge_triples_;
  int32_t v_tag_;
  physical::PhysicalOpr::MetaData meta_data_;
};

}  // namespace gs