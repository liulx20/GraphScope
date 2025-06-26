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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_VARIABLE_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_VARIABLE_H_

#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"

namespace gs {
namespace chunked_runtime {

class VariableExpr : public ExprBase {
 public:
  template <typename GraphInterface>
  VariableExpr(const GraphInterface& graph, const DataChunk& ctx,
               const common::Variable& pb, VarType var_type)
      : var_(graph, ctx, pb, var_type) {}

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena&) const override;
  RTAnyType type() const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
  RTAny eval_vertex(label_t label, vid_t v, Arena&, int) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena&, int) const override;

  bool is_optional() const override { return var_.is_optional(); }

 private:
  Var var_;
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_VARIABLE_H_