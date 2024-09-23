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

#include "flex/engines/graph_db/runtime/common/operators/join.h"
#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {

Context eval_join(const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params,
                  const physical::Join& opr, Context&& ctx, Context&& ctx2) {
  JoinParams p;

  auto left_keys = opr.left_keys();
  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(FATAL) << "left_keys should have tag";
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  auto right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(FATAL) << "right_keys should have tag";
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }
  if (opr.has_condition()) {
    // ctx2.set_prev_context(&ctx);
    LOG(FATAL) << "join with condition" << opr.condition().DebugString();
    // auto pred = parse_join_condition(txn, ctx, ctx2, params,
    // opr.condition());
    if (opr.join_kind() == physical::Join_JoinKind::Join_JoinKind_INNER) {
      p.join_type = JoinKind::kInnerJoin;
    }
    return ctx;
    // return Join::join(std::move(ctx), std::move(ctx2), p, pred);
  }
  switch (opr.join_kind()) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    p.join_type = JoinKind::kInnerJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    p.join_type = JoinKind::kSemiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    p.join_type = JoinKind::kAntiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    p.join_type = JoinKind::kLeftOuterJoin;
    break;
  default:
    LOG(FATAL) << "unsupported join kind" << opr.join_kind();
  }
  return Join::join(std::move(ctx), std::move(ctx2), p);
}
}  // namespace runtime
}  // namespace gs