#ifndef CODEGEN_SRC_HQPS_HQPS_EXPR_NEW_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_EXPR_NEW_BUILDER_H_

#include <stack>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

#include <boost/format.hpp>

namespace gs {

class ExprBuilderNEW {
 public:
  ExprBuilderNEW(BuildingContext& context) : context_(context) {}
  static int get_prority(const common::ExprOpr& opr) {
    switch (opr.item_case()) {
    case common::ExprOpr::kBrace: {
      return 1;
    }
    case common::ExprOpr::kExtract: {
      return 2;
    }
    case common::ExprOpr::kLogical: {
      switch (opr.logical()) {
      case common::Logical::AND:
        return 11;
      case common::Logical::OR:
        return 12;
      case common::Logical::NOT:
        return 2;
      case common::Logical::EQ:
      case common::Logical::NE:
        return 7;
      case common::Logical::GE:
      case common::Logical::GT:
      case common::Logical::LT:
      case common::Logical::LE:
        return 6;
      default:
        return 16;
      }
    }
    case common::ExprOpr::kArith: {
      switch (opr.arith()) {
      case common::Arithmetic::ADD:
      case common::Arithmetic::SUB:
        return 4;
      case common::Arithmetic::MUL:
      case common::Arithmetic::DIV:
      case common::Arithmetic::MOD:
        return 3;
      default:
        return 16;
      }
    }
    default:
      return 16;
    }
    return 16;
  }
  static std::pair<std::string, std::string> value_pb_to_str(
      const common::Value& value, size_t idx) {
    static constexpr const char* CONSTACCESSOR_TEMPLATE_STR =
        "ConstAccessor<%1%> expr%2%(%3%);\n";
    boost::format fmt(CONSTACCESSOR_TEMPLATE_STR);
    switch (value.item_case()) {
    case common::Value::kI32:
      fmt % "int32_t" % idx % value.i32();
      return {fmt.str(), "int32_t"};
    case common::Value::kI64:
      fmt % "int64_t" % idx % value.i64();
      return {fmt.str(), "int64_t"};
    case common::Value::kF64:
      fmt % "double" % idx % value.f64();
      return {fmt.str(), "double"};
    case common::Value::kStr:
      fmt % "std::string" % idx % ("\"" + value.str() + "\"");
      return {fmt.str(), "std::string"};
    case common::Value::kBoolean:
      fmt % "bool" % idx % (value.boolean() ? "true" : "false");
      return {fmt.str(), "bool"};
    case common::Value::kNone:
      fmt % "gs::None" % idx % "gs::None";
      return {fmt.str(), "gs::None"};
    case common::Value::kDate:
      fmt % "Date" % idx % ("Date(" + value.date().DebugString() + ")");
      return {fmt.str(), "Date"};
    default:
      throw std::runtime_error("unknown value type" + value.DebugString());
    }
  }

  static std::pair<std::string, std::string> param_pb_to_str(
      const common::DynamicParam& param, size_t idx) {
    static constexpr const char* PARAMACCESSOR_TEMPLATE_STR =
        "ParamAccessor<%1%> expr%2%(params, \"%3%\");\n";
    boost::format fmt(PARAMACCESSOR_TEMPLATE_STR);

    switch (param.data_type().data_type()) {
    case common::DataType::INT32:
      fmt % "int32_t" % idx % param.name();
      return {fmt.str(), "int32_t"};
    case common::DataType::INT64:
      fmt % "int64_t" % idx % param.name();
      return {fmt.str(), "int64_t"};
    case common::DataType::DOUBLE:
      fmt % "double" % idx % param.name();
      return {fmt.str(), "double"};
    case common::DataType::STRING:
      fmt % "std::string_view" % idx % param.name();
      return {fmt.str(), "std::string_view"};
    case common::DataType::BOOLEAN:
      fmt % "bool" % idx % param.name();
      return {fmt.str(), "bool"};
    case common::DataType::NONE:
      fmt % "gs::None" % idx % param.name();
      return {fmt.str(), "gs::None"};
    case common::DataType::DATE32:
      fmt % "Date" % idx % param.name();
      return {fmt.str(), "Date"};
    default:
      throw std::runtime_error("unknown param type" + param.DebugString());
    }
  }
  static std::string logical_2_str(const common::Logical& logi) {
    switch (logi) {
    case common::Logical::AND:
      return "AndOp";
    case common::Logical::LT:
      return "LTOp";
    case common::Logical::LE:
      return "LEOp";
    case common::Logical::GT:
      return "GTOp";
    case common::Logical::GE:
      return "GEOp";
    case common::Logical::EQ:
      return "EQOp";
    case common::Logical::NE:
      return "NEOp";
    case common::Logical::OR:
      return "OrOp";
    default:
      throw std::runtime_error("unknown logical opr");
    }
  }
  static std::string arith_2_str(const common::Arithmetic& arith) {
    switch (arith) {
    case common::Arithmetic::ADD:
      return "AddOp";
    case common::Arithmetic::SUB:
      return "SubOp";
    case common::Arithmetic::MUL:
      return "MulOp";
    case common::Arithmetic::DIV:
      return "DivOp";
    case common::Arithmetic::MOD:
      return "ModOp";
    default:
      throw std::runtime_error("unknown arith opr");
    }
  }

  static std::string extract_interval_2_str(
      const common::Extract::Interval& extract) {
    switch (extract) {
    case common::Extract::YEAR:
      return "Year";
    case common::Extract::MONTH:
      return "Month";
    case common::Extract::DAY:
      return "Day";
    case common::Extract::HOUR:
      return "Hour";
    case common::Extract::MINUTE:
      return "Minute";
    case common::Extract::SECOND:
      return "Second";
    case common::Extract::MILLISECOND:
      return "Millisecond";
    default:
      throw std::runtime_error("unknown extract opr");
    }
  }
  std::pair<std::string, std::string> var_pb_to_str(const common::Variable& var,
                                                    size_t idx) {
    auto ctx_name = context_.GetCurCtxName();

    if (var.has_property()) {
      auto prop = var.property();
      if (prop.has_key()) {
        if (var.has_tag()) {
          if (prop.key().name() == "id") {
            return {"VertexIdPathAccessor expr" + std::to_string(idx) +
                        "(txn, " + ctx_name + ", " +
                        std::to_string(var.tag().id()) + ");\n",
                    "int64_t"};
          } else {
            return {
                "VertexPropertyPathAccessor<" +
                    single_common_data_type_pb_2_str(
                        var.node_type().data_type()) +
                    "> " + "expr" + std::to_string(idx) + "(txn, " + ctx_name +
                    ", " + std::to_string(var.tag().id()) + ", \"" +
                    prop.key().name() + "\");\n",
                single_common_data_type_pb_2_str(var.node_type().data_type())};
          }
        } else {
          if (prop.key().name() == "id") {
            return {"VertexIdVertexAccessor expr" + std::to_string(idx) +
                        "(txn);\n",
                    "int64_t"};
          } else {
            return {
                "VertexPropertyVertexAccessor<" +
                    single_common_data_type_pb_2_str(
                        var.node_type().data_type()) +
                    "> " + "expr" + std::to_string(idx) + "(txn, \"" +
                    prop.key().name() + "\");\n",
                single_common_data_type_pb_2_str(var.node_type().data_type())};
          }
        }
      } else if (prop.has_label()) {
      } else {
        throw std::runtime_error("unknown var property" + var.DebugString());
      }
    } else {
      return {
          "ContextAccessor<" +
              single_common_data_type_pb_2_str(var.node_type().data_type()) +
              "> expr" + std::to_string(idx) + "(txn, " +
              std::to_string(var.tag().id()) + ");\n",
          single_common_data_type_pb_2_str(var.node_type().data_type())};
    }
    return {"", ""};
  }
  std::string evalExpression(std::stack<common::ExprOpr>& opr_stack,
                             std::vector<std::string>& res) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case common::ExprOpr::kConst: {
      auto [str, type] = value_pb_to_str(opr.const_(), res.size());
      res.emplace_back(str);
      return type;
    }

    case common::ExprOpr::kVar: {
      auto [str, type] = var_pb_to_str(opr.var(), res.size());
      res.emplace_back(str);
      return type;
    }
    case common::ExprOpr::kParam: {
      auto [str, type] = param_pb_to_str(opr.param(), res.size());
      res.emplace_back(str);
      return type;
    }
    case common::ExprOpr::kExtract: {
      auto left = evalExpression(opr_stack, res);
      size_t lhs = res.size() - 1;
      if (left != "date") {
        throw std::runtime_error("extract only support date type");
      } else {
        auto val = extract_interval_2_str(opr.extract().interval());
        res.emplace_back("DateTimeExtractor" + val + "Expr expr" +
                         std::to_string(res.size()) + "(expr" +
                         std::to_string(lhs) + ");\n");
        return "int32_t";
      }
    }
    case common::ExprOpr::kLogical: {
      switch (opr.logical()) {
      case common::Logical::AND:
      case common::Logical::OR: {
        auto left = evalExpression(opr_stack, res);
        size_t lhs = res.size() - 1;
        auto right = evalExpression(opr_stack, res);
        size_t rhs = res.size() - 1;

        res.emplace_back("BinaryOpExpr expr" + std::to_string(res.size()) +
                         "(expr" + std::to_string(lhs) + ", expr" +
                         std::to_string(rhs) + ", " +
                         logical_2_str(opr.logical()) + ");\n");
        return "bool";
      }
      case common::Logical::EQ:
      case common::Logical::NE:
      case common::Logical::GE:
      case common::Logical::GT:
      case common::Logical::LT:
      case common::Logical::LE: {
        auto left = evalExpression(opr_stack, res);
        size_t lhs = res.size() - 1;
        auto right = evalExpression(opr_stack, res);
        size_t rhs = res.size() - 1;

        res.emplace_back("BinaryOpExpr expr" + std::to_string(res.size()) +
                         "(expr" + std::to_string(lhs) + ", expr" +
                         std::to_string(rhs) + ", " +
                         logical_2_str(opr.logical()) + "<" + left + ">);\n");
        return "bool";
      }
      default:
        throw std::runtime_error("unknown logical opr" + opr.DebugString());
      }
      break;
    }
    case common::ExprOpr::kArith: {
      switch (opr.arith()) {
      case common::Arithmetic::ADD:
      case common::Arithmetic::SUB:
      case common::Arithmetic::MUL:
      case common::Arithmetic::DIV:
      case common::Arithmetic::MOD: {
        auto left = evalExpression(opr_stack, res);
        size_t lhs = res.size() - 1;
        auto right = evalExpression(opr_stack, res);
        size_t rhs = res.size() - 1;

        res.emplace_back("BinaryOpExpr expr" + std::to_string(res.size()) +
                         "(expr" + std::to_string(lhs) + ", expr" +
                         std::to_string(rhs) + ", " + arith_2_str(opr.arith()) +
                         "<" + left + ">);\n");
        return left;
      }
      default:
        throw std::runtime_error("unknown arith opr" + opr.DebugString());
      }
    }
    default:
      break;
    }

    return "";
  }

  std::pair<std::string, std::string> AddAllExprOpr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& expr_oprs) {
    std::stack<common::ExprOpr> opr_stack;
    std::stack<common::ExprOpr> opr_stack2;
    for (auto it = expr_oprs.rbegin(); it != expr_oprs.rend(); ++it) {
      switch ((*it).item_case()) {
      case common::ExprOpr::kBrace: {
        auto brace = (*it).brace();
        if (brace == common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
          // left
          while (!opr_stack.empty() &&
                 opr_stack.top().item_case() != common::ExprOpr::kBrace) {
            opr_stack2.emplace(opr_stack.top());
            opr_stack.pop();
          }
          opr_stack.pop();
        } else {
          // right
          opr_stack.emplace(*it);
        }
        break;
      }
      case common::ExprOpr::kConst:
      case common::ExprOpr::kVar:
      case common::ExprOpr::kVars:
      case common::ExprOpr::kParam: {
        opr_stack2.emplace(*it);
        break;
      }

      case common::ExprOpr::kExtract: {
        opr_stack2.emplace(*it);
        break;
      }

      case common::ExprOpr::kLogical:
      case common::ExprOpr::kArith: {
        while (true) {
          if (opr_stack.empty() ||
              opr_stack.top().item_case() == common::ExprOpr::kBrace) {
            opr_stack.emplace(*it);
            break;
          }
          if (get_prority(*it) <= get_prority(opr_stack.top())) {
            opr_stack.emplace(*it);
            break;
          } else {
            opr_stack2.emplace(opr_stack.top());
            opr_stack.pop();
          }
        }
        break;
      }
      default:
        break;
      }
    }
    while (!opr_stack.empty()) {
      opr_stack2.emplace(opr_stack.top());
      opr_stack.pop();
    }
    std::vector<std::string> vec{};
    evalExpression(opr_stack2, vec);
    size_t len = vec.size();
    std::string rt{};
    for (auto str : vec) {
      rt += str;
    }
    return {rt, "expr" + std::to_string(len - 1)};
  }

 private:
  BuildingContext& context_;
};

}  // namespace gs
#endif