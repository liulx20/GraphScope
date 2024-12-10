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

#ifndef RUNTIME_ADHOC_OPERATORS_UPDATE_OPERATORS_H_
#define RUNTIME_ADHOC_OPERATORS_UPDATE_OPERATORS_H_

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"

namespace gs {

namespace runtime {

WriteContext eval_project(const physical::Project& opr,
                          const GraphInsertInterface& graph, WriteContext&& ctx,
                          const std::map<std::string, std::string>& params);

WriteContext eval_load(const cypher::Load& opr, GraphInsertInterface& graph,
                       WriteContext&& ctx,
                       const std::map<std::string, std::string>& params);

WriteContext eval_unfold(const physical::Unfold& opr, WriteContext&& ctx);

WriteContext eval_dedup(const algebra::Dedup& opr,
                        const GraphInsertInterface& graph, WriteContext&& ctx);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_UPDATE_OPERATORS_H_