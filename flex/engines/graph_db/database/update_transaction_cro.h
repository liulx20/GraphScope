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

#ifndef GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_CRO_H_
#define GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_CRO_H_

#include <limits>
#include <utility>

#include "flat_hash_map/flat_hash_map.hpp"
#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"

namespace gs {

class MutablePropertyFragment;
class MMapAllocator;
class WalWriter;
class VersionManager;

class UpdateTransactionCRO {
 public:
  UpdateTransactionCRO(MutablePropertyFragment& graph, MMapAllocator& alloc,
                       WalWriter& logger, VersionManager& vm,
                       timestamp_t timestamp);

  ~UpdateTransactionCRO();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  bool UpdateVertex(label_t label, const Any& oid, vid_t vid,
                    std::vector<Any>&& out);
  bool UpdateEdge(label_t src_label, const Any& src, label_t dst_label,
                  const Any& dst, label_t edge_label, const Any& prop,
                  std::shared_ptr<MutableCsrEdgeIterBase>& in_edge,
                  std::shared_ptr<MutableCsrEdgeIterBase>& out_edge);

 private:
  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  MutablePropertyFragment& graph_;
  MMapAllocator& alloc_;
  WalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;
  std::vector<std::tuple<label_t, vid_t, std::vector<Any>>> update_verties_;
  std::vector<std::tuple<std::shared_ptr<MutableCsrEdgeIterBase>,
                         std::shared_ptr<MutableCsrEdgeIterBase>, Any>>
      update_edges_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_H_