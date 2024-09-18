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

#ifndef RUNTIME_ADHOC_OPERATORS_OPERATORS_H_
#define RUNTIME_ADHOC_OPERATORS_OPERATORS_H_

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/utils/app_utils.h"

namespace gs {

namespace runtime {

Context eval_dedup(const algebra::Dedup& opr, const ReadTransaction& txn,
                   Context&& ctx);

Context eval_group_by(const physical::GroupBy& opr, const ReadTransaction& txn,
                      Context&& ctx);

Context eval_order_by(const algebra::OrderBy& opr, const ReadTransaction& txn,
                      Context&& ctx, bool enable_staged = true);

Context eval_path_expand_v(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias);

Context eval_path_expand_p(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias);

bool project_order_by_fusable(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const Context& ctx, const std::vector<common::IrDataType>& data_types);

Context eval_shortest_path(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           const physical::GetV& get_v_opr, int);

Context eval_all_shortest_paths(
    const physical::PathExpand& opr, const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta, const physical::GetV& get_v_opr,
    int);

Context eval_project(const physical::Project& opr, const ReadTransaction& txn,
                     Context&& ctx,
                     const std::map<std::string, std::string>& params,
                     const std::vector<common::IrDataType>& data_types);

Context eval_project_order_by(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const std::vector<common::IrDataType>& data_types);

Context eval_scan(const physical::Scan& scan_opr, const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params);

Context eval_select(const algebra::Select& opr, const ReadTransaction& txn,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params);

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const ReadTransaction& txn, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         const physical::PhysicalOpr_MetaData& meta, int op_id);

bool edge_expand_get_v_fusable(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr, const Context& ctx,
                               const physical::PhysicalOpr_MetaData& meta);

Context eval_edge_expand_get_v(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr,
                               const ReadTransaction& txn, Context&& ctx,
                               const std::map<std::string, std::string>& params,
                               const physical::PhysicalOpr_MetaData& meta,
                               int op_id);

Context eval_get_v(const physical::GetV& opr, const ReadTransaction& txn,
                   Context&& ctx,
                   const std::map<std::string, std::string>& params);

Context eval_intersect(const ReadTransaction& txn,
                       const physical::Intersect& opr, Context&& ctx,
                       std::vector<Context>&& ctxs);

Context eval_join(const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params,
                  const physical::Join& opr, Context&& ctx, Context&& ctx2);

Context eval_limit(const algebra::Limit& opr, Context&& ctx);

Context eval_unfold(const physical::Unfold& opr, Context&& ctx);

void eval_sink(const Context& ctx, const ReadTransaction& txn, Encoder& output);

void eval_sink_beta(const Context& ctx, const ReadTransaction& txn,
                    Encoder& output);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_OPERATORS_H_