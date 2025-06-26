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
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::RTAnyType;
using SPPredicateType = gs::runtime::SPPredicateType;
bl::result<void> Scan::find_vertex_with_oid(const GraphReadInterface& graph,
                                            label_t label, const Any& oid,
                                            int32_t alias,
                                            ScanOprState& state) {
  state.start_label(label);
  vid_t vid;
  if (graph.GetVertexIndex(label, oid, vid)) {
    state.collect(vid);
  }
  state.set_initialized(true);
  return bl::result<void>();
}

bl::result<void> Scan::find_vertex_with_gid(const GraphReadInterface& graph,
                                            label_t label, int64_t gid,
                                            int32_t alias,
                                            ScanOprState& state) {
  state.start_label(label);
  if (GlobalId::get_label_id(gid) == label) {
    state.collect(GlobalId::get_vid(gid));
  } else {
    LOG(ERROR) << "Invalid label id: "
               << static_cast<int>(GlobalId::get_label_id(gid));
  }
  state.set_initialized(true);
  return bl::result<void>();
}

template <typename T>
static bl::result<void> _scan_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, ScanOprState& state) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyEQPredicateBeta<T>&>(
            pred),
        state);
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGEPredicateBeta<T>&>(
            pred),
        state);
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGTPredicateBeta<T>&>(
            pred),
        state);
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLEPredicateBeta<T>&>(
            pred),
        state);
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLTPredicateBeta<T>&>(
            pred),
        state);
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::scan_vertex<gs::runtime::VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyNEPredicateBeta<T>&>(
            pred),
        state);
  } else {
    LOG(ERROR) << "not impl... - " << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR(
        "not support vertex special property predicate type");
  }
}

bl::result<void> Scan::scan_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, ScanOprState& state) {
  if (pred.data_type() == RTAnyType::kI64Value) {
    return _scan_vertex_with_special_vertex_predicate<int64_t>(graph, params,
                                                               pred, state);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _scan_vertex_with_special_vertex_predicate<int32_t>(graph, params,
                                                               pred, state);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _scan_vertex_with_special_vertex_predicate<std::string_view>(
        graph, params, pred, state);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _scan_vertex_with_special_vertex_predicate<double>(graph, params,
                                                              pred, state);
  } else if (pred.data_type() == RTAnyType::kDate32) {
    return _scan_vertex_with_special_vertex_predicate<Day>(graph, params, pred,
                                                           state);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _scan_vertex_with_special_vertex_predicate<Date>(graph, params, pred,
                                                            state);
  } else {
    LOG(ERROR) << "not impl - " << static_cast<int>(pred.data_type());
    RETURN_UNSUPPORTED_ERROR(
        "not support vertex special property predicate type");
  }
}

template <typename T>
static bl::result<void> _filter_gids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, const std::vector<int64_t>& gids,
    ScanOprState& state) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::filter_gids<gs::runtime::VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyEQPredicateBeta<T>&>(
            pred),
        gids, state);
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::filter_gids<gs::runtime::VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGEPredicateBeta<T>&>(
            pred),
        gids, state);
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::filter_gids<gs::runtime::VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGTPredicateBeta<T>&>(
            pred),
        gids, state);
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::filter_gids<gs::runtime::VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLEPredicateBeta<T>&>(
            pred),
        gids, state);
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::filter_gids<gs::runtime::VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLTPredicateBeta<T>&>(
            pred),
        gids, state);
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::filter_gids<gs::runtime::VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyNEPredicateBeta<T>&>(
            pred),
        gids, state);
  } else {
    LOG(ERROR) << "not impl... - " << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR(
        "not support vertex special property predicate type");
  }
}

bl::result<void> Scan::filter_gids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& predicate, const std::vector<int64_t>& oids,
    ScanOprState& state) {
  if (predicate.data_type() == RTAnyType::kI64Value) {
    return _filter_gids_with_special_vertex_predicate<int64_t>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kI32Value) {
    return _filter_gids_with_special_vertex_predicate<int32_t>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kStringValue) {
    return _filter_gids_with_special_vertex_predicate<std::string_view>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kF64Value) {
    return _filter_gids_with_special_vertex_predicate<double>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kDate32) {
    return _filter_gids_with_special_vertex_predicate<Day>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kTimestamp) {
    return _filter_gids_with_special_vertex_predicate<Date>(
        graph, params, predicate, oids, state);
  } else {
    LOG(ERROR) << "not support type: "
               << static_cast<int>(predicate.data_type());
    RETURN_UNSUPPORTED_ERROR("not support vertex property type");
  }
}

template <typename T>
static bl::result<void> _filter_oid_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, const std::vector<Any>& oids,
    ScanOprState& state) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::filter_oids<gs::runtime::VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyEQPredicateBeta<T>&>(
            pred),
        oids, state);
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::filter_oids<gs::runtime::VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGEPredicateBeta<T>&>(
            pred),
        oids, state);
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::filter_oids<gs::runtime::VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyGTPredicateBeta<T>&>(
            pred),
        oids, state);
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::filter_oids<gs::runtime::VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLEPredicateBeta<T>&>(
            pred),
        oids, state);
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::filter_oids<gs::runtime::VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyLTPredicateBeta<T>&>(
            pred),
        oids, state);
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::filter_oids<gs::runtime::VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const gs::runtime::VertexPropertyNEPredicateBeta<T>&>(
            pred),
        oids, state);
  } else {
    LOG(ERROR) << "not impl... - " << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR(
        "not support vertex special property predicate type");
  }
}

bl::result<void> Scan::filter_oids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& predicate, const std::vector<Any>& oids,
    ScanOprState& state) {
  if (predicate.data_type() == RTAnyType::kI64Value) {
    return _filter_oid_with_special_vertex_predicate<int64_t>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kI32Value) {
    return _filter_oid_with_special_vertex_predicate<int32_t>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kStringValue) {
    return _filter_oid_with_special_vertex_predicate<std::string_view>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kF64Value) {
    return _filter_oid_with_special_vertex_predicate<double>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kDate32) {
    return _filter_oid_with_special_vertex_predicate<Day>(
        graph, params, predicate, oids, state);
  } else if (predicate.data_type() == RTAnyType::kTimestamp) {
    return _filter_oid_with_special_vertex_predicate<Date>(
        graph, params, predicate, oids, state);
  } else {
    LOG(ERROR) << "not support type: "
               << static_cast<int>(predicate.data_type());
    RETURN_UNSUPPORTED_ERROR("not support vertex property type");
  }
}
}  // namespace ops
}  // namespace chunked_runtime

}  // namespace gs
