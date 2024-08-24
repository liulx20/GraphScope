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

#include "flex/engines/graph_db/runtime/common/operators/get_v.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

VOpt parse_opt(const physical::GetV_VOpt& opt) {
  if (opt == physical::GetV_VOpt::GetV_VOpt_START) {
    return VOpt::kStart;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_END) {
    return VOpt::kEnd;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_OTHER) {
    return VOpt::kOther;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_BOTH) {
    return VOpt::kBoth;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
    return VOpt::kItself;
  } else {
    LOG(FATAL) << "unexpected GetV::Opt";
    return VOpt::kItself;
  }
}

Context eval_get_v(const physical::GetV& opr, const ReadTransaction& txn,
                   Context&& ctx,
                   const std::map<std::string, std::string>& params) {
  int tag = -1;
  if (opr.has_tag()) {
    tag = opr.tag().value();
  }
  VOpt opt = parse_opt(opr.opt());
  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  if (opr.has_params()) {
    const algebra::QueryParams& query_params = opr.params();
    GetVParams p;
    p.opt = opt;
    p.tag = tag;
    p.tables = parse_tables(query_params);
    p.alias = alias;
    if (query_params.has_predicate()) {
      if (opt == VOpt::kItself) {
        std::set<label_t> labels_set;
        label_t exact_pk_label;
        Any exact_pk;
        if (is_label_within_predicate(query_params.predicate(), labels_set)) {
          std::shared_ptr<IVertexColumn> input_vertex_list_ptr =
              std::dynamic_pointer_cast<IVertexColumn>(ctx.get(p.tag));
          bool within = true;
          for (auto label : input_vertex_list_ptr->get_labels_set()) {
            if (labels_set.find(label) == labels_set.end()) {
              within = false;
              break;
            }
          }
          if (p.tag == -1 && within) {
            ctx.set(p.alias, input_vertex_list_ptr);
            return ctx;
          } else {
            GeneralVertexPredicate pred(txn, ctx, params,
                                        query_params.predicate());
            return GetV::get_vertex_from_vertices(txn, std::move(ctx), p, pred);
          }
        } else if (is_pk_exact_check(query_params.predicate(), params,
                                     exact_pk_label, exact_pk)) {
          vid_t index = std::numeric_limits<vid_t>::max();
          txn.GetVertexIndex(exact_pk_label, exact_pk, index);
          ExactVertexPredicate pred(exact_pk_label, index);

          return GetV::get_vertex_from_vertices(txn, std::move(ctx), p, pred);
        } else {
          GeneralVertexPredicate pred(txn, ctx, params,
                                      query_params.predicate());
          return GetV::get_vertex_from_vertices(txn, std::move(ctx), p, pred);
        }
      } else if (opt == VOpt::kEnd || opt == VOpt::kStart) {
        GeneralVertexPredicate pred(txn, ctx, params, query_params.predicate());
        return GetV::get_vertex_from_edges(txn, std::move(ctx), p, pred);
      }
    } else {
      if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
        auto ret = GetV::get_vertex_from_edges(txn, std::move(ctx), p,
                                               DummyVertexPredicate());
        return ret;
      }
    }
  }

  LOG(FATAL) << "not support";
  return ctx;
}

}  // namespace runtime

}  // namespace gs