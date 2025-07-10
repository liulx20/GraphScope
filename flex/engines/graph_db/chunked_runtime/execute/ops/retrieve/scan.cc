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

#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/scan.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/chunked_runtime/execute/operator.h"
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/scan.h"
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_parser.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/scan_utils.h"
namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::ops::ScanUtils;

typedef const std::map<std::string, std::string>& ParamsType;
class FilterOidsWithoutPredOpr : public IReadOpr {
 public:
  FilterOidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Any>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    std::vector<Any> oids = oids_(params);
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    if (params_.tables.size() == 1 && oids.size() == 1) {
      return Scan::find_vertex_with_oid(graph, params_.tables[0], oids[0],
                                        params_.alias, casted_state);
    }
    return Scan::filter_oids(
        graph, params_, [](label_t, vid_t) { return true; }, oids,
        casted_state);
  }

  std::string get_operator_name() const override { return "FilterOidsOpr"; }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
};

class FilterMultiTypeOidsWithoutPredOpr : public IReadOpr {
 public:
  FilterMultiTypeOidsWithoutPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids)
      : params_(params), oids_(oids) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    std::vector<Any> oids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        oids.push_back(o);
      }
    }
    return Scan::filter_oids(
        graph, params_, [](label_t, vid_t) { return true; }, oids,
        casted_state);
  }

  std::string get_operator_name() const override { return "FilterOidsOpr"; }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
};

class FilterGidsWithoutPredOpr : public IReadOpr {
 public:
  FilterGidsWithoutPredOpr(
      ScanParams params,
      const std::function<std::vector<Any>(ParamsType)>& oids)
      : params_(params), oids_(std::move(oids)) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }
    if (params_.tables.size() == 1 && gids.size() == 1) {
      return Scan::find_vertex_with_gid(graph, params_.tables[0], gids[0],
                                        params_.alias, casted_state);
    }
    return Scan::filter_gids(
        graph, params_, [](label_t, vid_t) { return true; }, gids,
        casted_state);
  }

  std::string get_operator_name() const override { return "FilterGidsOpr"; }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
};

class FilterOidsSPredOpr : public IReadOpr {
 public:
  FilterOidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const std::function<std::unique_ptr<SPVertexPredicate>(
                         const gs::runtime::GraphReadInterface&,
                         const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    auto ids = oids_(params);
    auto pred = pred_(graph, params);
    return Scan::filter_oids_with_special_vertex_predicate(
        graph, params_, *pred, ids, casted_state);
  }

  std::string get_operator_name() const override {
    return "FilterOidsSPredOpr";
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterOidsGPredOpr : public IReadOpr {
 public:
  FilterOidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    auto ids = oids_(params);
    Arena arena;
    DataChunk chunk;
    auto expr =
        parse_expression(graph, chunk, params, pred_, VarType::kVertexVar);

    if (expr->is_optional()) {
      return Scan::filter_oids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena, 0).as_bool();
          },
          ids, casted_state);
    } else {
      return Scan::filter_oids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena).as_bool();
          },
          ids, casted_state);
    }
  }

  std::string get_operator_name() const override {
    return "FilterOidsGPredOpr";
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  common::Expression pred_;
};

class FilterOidsMultiTypeSPredOpr : public IReadOpr {
 public:
  FilterOidsMultiTypeSPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids,
      const std::function<std::unique_ptr<SPVertexPredicate>(
          const gs::runtime::GraphReadInterface&,
          const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(oids), pred_(pred) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    std::vector<Any> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    auto pred = pred_(graph, params);
    return Scan::filter_oids_with_special_vertex_predicate(
        graph, params_, *pred, all_ids, casted_state);
  }

  std::string get_operator_name() const override {
    return "FilterOidsMultiTypeSPredOpr";
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterOidsMultiTypeGPredOpr : public IReadOpr {
 public:
  FilterOidsMultiTypeGPredOpr(
      ScanParams params,
      const std::vector<std::function<std::vector<Any>(ParamsType)>>& oids,
      const common::Expression& pred)
      : params_(params), oids_(oids), pred_(pred) {}

  std::string get_operator_name() const override {
    return "FilterOidsMultiTypeGPredOpr";
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    std::vector<Any> all_ids;
    for (auto& _oid : oids_) {
      auto oid = _oid(params);
      for (auto& o : oid) {
        all_ids.push_back(o);
      }
    }
    DataChunk chunk;
    auto expr =
        parse_expression(graph, chunk, params, pred_, VarType::kVertexVar);
    Arena arena;
    if (expr->is_optional()) {
      return Scan::filter_oids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena, 0).as_bool();
          },
          all_ids, casted_state);
    } else {
      return Scan::filter_oids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena).as_bool();
          },
          all_ids, casted_state);
    }
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::vector<std::function<std::vector<Any>(ParamsType)>> oids_;
  common::Expression pred_;
};

class FilterGidsSPredOpr : public IReadOpr {
 public:
  FilterGidsSPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const std::function<std::unique_ptr<SPVertexPredicate>(
                         const gs::runtime::GraphReadInterface&,
                         const std::map<std::string, std::string>&)>& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  std::string get_operator_name() const override {
    return "FilterGidsSPredOpr";
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    auto ids = oids_(params);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }
    auto pred = pred_(graph, params);
    return Scan::filter_gids_with_special_vertex_predicate(
        graph, params_, *pred, gids, casted_state);
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class FilterGidsGPredOpr : public IReadOpr {
 public:
  FilterGidsGPredOpr(ScanParams params,
                     const std::function<std::vector<Any>(ParamsType)>& oids,
                     const common::Expression& pred)
      : params_(params), oids_(std::move(oids)), pred_(pred) {}

  std::string get_operator_name() const override {
    return "FilterGidsGPredOpr";
  }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              ParamsType params, IOprState& state) override {
    auto ids = oids_(params);
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    std::vector<int64_t> gids;
    for (size_t i = 0; i < ids.size(); i++) {
      gids.push_back(ids[i].AsInt64());
    }

    Arena arena;
    DataChunk chunk;

    auto expr =
        parse_expression(graph, chunk, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      return Scan::filter_gids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena, 0).as_bool();
          },
          gids, casted_state);
    } else {
      return Scan::filter_gids(
          graph, params_,
          [&expr, &arena](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, arena).as_bool();
          },
          gids, casted_state);
    }
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams params_;
  std::function<std::vector<Any>(ParamsType)> oids_;
  common::Expression pred_;
};

class ScanWithSPredOpr : public IReadOpr {
 public:
  ScanWithSPredOpr(const ScanParams& scan_params,
                   std::function<std::unique_ptr<SPVertexPredicate>(
                       const gs::runtime::GraphReadInterface&,
                       const std::map<std::string, std::string>&)>
                       pred)
      : scan_params_(scan_params), pred_(pred) {}

  std::string get_operator_name() const override { return "ScanWithSPredOpr"; }

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    auto pred = pred_(graph, params);
    return Scan::scan_vertex_with_special_vertex_predicate(graph, scan_params_,
                                                           *pred, casted_state);
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams scan_params_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const gs::runtime::GraphReadInterface&,
      const std::map<std::string, std::string>&)>
      pred_;
};

class ScanWithGPredOpr : public IReadOpr {
 public:
  ScanWithGPredOpr(const ScanParams& scan_params,
                   const common::Expression& pred)
      : scan_params_(scan_params), pred_(pred) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    Arena arena;
    DataChunk chunk;
    auto expr =
        parse_expression(graph, chunk, params, pred_, VarType::kVertexVar);
    if (expr->is_optional()) {
      if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
        return Scan::scan_vertex(
            graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, arena, 0).as_bool();
            },
            casted_state);
      } else {
        return Scan::scan_vertex_with_limit(
            graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, arena, 0).as_bool();
            },
            casted_state);
      }
    } else {
      if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
        auto ret = Scan::scan_vertex(
            graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, arena).as_bool();
            },
            casted_state);
        return ret;
      } else {
        auto ret = Scan::scan_vertex_with_limit(
            graph, scan_params_,
            [&expr, &arena](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, arena).as_bool();
            },
            casted_state);
        return ret;
      }
    }
  }
  std::string get_operator_name() const override { return "ScanWithGPredOpr"; }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams scan_params_;
  common::Expression pred_;
};

class ScanWithoutPredOpr : public IReadOpr {
 public:
  ScanWithoutPredOpr(const ScanParams& scan_params)
      : scan_params_(scan_params) {}

  bl::result<void> EvalChunks(const gs::runtime::GraphReadInterface& graph,
                              const std::map<std::string, std::string>& params,
                              IOprState& state) override {
    auto& casted_state = dynamic_cast<ScanOprState&>(state);
    if (scan_params_.limit == std::numeric_limits<int32_t>::max()) {
      return Scan::scan_vertex(
          graph, scan_params_, [](label_t, vid_t) { return true; },
          casted_state);
    } else {
      return Scan::scan_vertex_with_limit(
          graph, scan_params_, [](label_t, vid_t) { return true; },
          casted_state);
    }
  }

  std::string get_operator_name() const override {
    return "ScanWithoutPredOpr";
  }

  std::shared_ptr<IOprState> initState(std::shared_ptr<IOprState>,
                                       const LocalMemPool& mem_pool) override {
    return std::make_shared<ScanOprState>(mem_pool);
  }

 private:
  ScanParams scan_params_;
};

bl::result<ReadOpBuildResultT> ScanOprBuilder::Build(
    std::unique_ptr<IReadOpr>&, const gs::Schema& schema,
    const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
    int op_idx) {
  int alias = -1;
  ContextMeta ret_meta;
  if (plan.plan(op_idx).opr().scan().has_alias()) {
    alias = plan.plan(op_idx).opr().scan().alias().value();
  }
  ret_meta.set(alias, gs::runtime::ContextColumnType::kVertex,
               gs::runtime::RTAnyType::kVertex);

  // ret_meta.set(alias);
  auto scan_opr = plan.plan(op_idx).opr().scan();
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    LOG(ERROR) << "Currently only support scan vertex";
    return std::make_pair(nullptr, ret_meta);
  }
  if (!scan_opr.has_params()) {
    LOG(ERROR) << "Scan operator should have params";
    return std::make_pair(nullptr, ret_meta);
  }

  gs::runtime::ScanParams scan_params;
  scan_params.alias = scan_opr.has_alias() ? scan_opr.alias().value() : -1;
  scan_params.limit = std::numeric_limits<int32_t>::max();
  if (scan_opr.params().has_limit()) {
    auto& limit_range = scan_opr.params().limit();
    if (limit_range.lower() != 0) {
      LOG(FATAL) << "Scan with lower limit expect 0, but got "
                 << limit_range.lower();
    }
    if (limit_range.upper() > 0) {
      scan_params.limit = limit_range.upper();
    }
  }
  for (auto& table : scan_opr.params().tables()) {
    // bug here, exclude invalid vertex label id
    if (schema.vertex_label_num() <= table.id()) {
      continue;
    }
    scan_params.tables.emplace_back(table.id());
  }
  if (scan_opr.has_idx_predicate()) {
    bool scan_oid = false;
    if (!ScanUtils::check_idx_predicate(scan_opr, scan_oid)) {
      LOG(ERROR) << "Index predicate is not supported"
                 << scan_opr.DebugString();
      return std::make_pair(nullptr, ret_meta);
    }
    // only one label and without predicate
    if (scan_params.tables.size() == 1 && scan_oid &&
        (!scan_opr.params().has_predicate())) {
      const auto& pks = schema.get_vertex_primary_key(scan_params.tables[0]);
      const auto& [type, _, __] = pks[0];
      auto oids =
          ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
      return std::make_pair(
          std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids),
          ret_meta);
    }

    // without predicate
    if (!scan_opr.params().has_predicate()) {
      if (!scan_oid) {
        auto gids = ScanUtils::parse_ids_with_type(PropertyType::kInt64,
                                                   scan_opr.idx_predicate());
        return std::make_pair(
            std::make_unique<FilterGidsWithoutPredOpr>(scan_params, gids),
            ret_meta);
      } else {
        std::vector<std::function<std::vector<Any>(ParamsType)>> oids;
        std::set<int> types;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          int type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            types.insert(type_impl);
            const auto& oid =
                ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          return std::make_pair(
              std::make_unique<FilterOidsWithoutPredOpr>(scan_params, oids[0]),
              ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterMultiTypeOidsWithoutPredOpr>(scan_params,
                                                                  oids),
              ret_meta);
        }
      }
    } else {
      auto sp_vertex_pred = gs::runtime::parse_special_vertex_predicate(
          scan_opr.params().predicate());
      if (scan_oid) {
        std::set<int> types;
        std::vector<std::function<std::vector<Any>(ParamsType)>> oids;
        for (auto& table : scan_params.tables) {
          const auto& pks = schema.get_vertex_primary_key(table);
          const auto& [type, _, __] = pks[0];
          auto type_impl = static_cast<int>(type.type_enum);
          if (types.find(type_impl) == types.end()) {
            auto oid =
                ScanUtils::parse_ids_with_type(type, scan_opr.idx_predicate());
            types.insert(type_impl);
            oids.emplace_back(oid);
          }
        }
        if (types.size() == 1) {
          if (sp_vertex_pred.has_value()) {
            return std::make_pair(std::make_unique<FilterOidsSPredOpr>(
                                      scan_params, oids[0], *sp_vertex_pred),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsGPredOpr>(
                    scan_params, oids[0], scan_opr.params().predicate()),
                ret_meta);
          }
        } else {
          if (sp_vertex_pred.has_value()) {
            return std::make_pair(std::make_unique<FilterOidsMultiTypeSPredOpr>(
                                      scan_params, oids, *sp_vertex_pred),
                                  ret_meta);
          } else {
            return std::make_pair(
                std::make_unique<FilterOidsMultiTypeGPredOpr>(
                    scan_params, oids, scan_opr.params().predicate()),
                ret_meta);
          }
        }

      } else {
        auto gids = ScanUtils::parse_ids_with_type(PropertyType::kInt64,
                                                   scan_opr.idx_predicate());
        if (sp_vertex_pred.has_value()) {
          return std::make_pair(std::make_unique<FilterGidsSPredOpr>(
                                    scan_params, gids, *sp_vertex_pred),
                                ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<FilterGidsGPredOpr>(
                  scan_params, gids, scan_opr.params().predicate()),
              ret_meta);
        }
      }
    }

  } else {
    if (scan_opr.params().has_predicate()) {
      auto sp_vertex_pred = gs::runtime::parse_special_vertex_predicate(
          scan_opr.params().predicate());
      if (sp_vertex_pred.has_value()) {
        return std::make_pair(
            std::make_unique<ScanWithSPredOpr>(scan_params, *sp_vertex_pred),
            ret_meta);
      } else {
        return std::make_pair(std::make_unique<ScanWithGPredOpr>(
                                  scan_params, scan_opr.params().predicate()),
                              ret_meta);
      }
    } else {
      return std::make_pair(std::make_unique<ScanWithoutPredOpr>(scan_params),
                            ret_meta);
    }
  }
}

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs