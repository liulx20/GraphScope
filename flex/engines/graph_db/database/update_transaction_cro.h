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
  bool AddVertexAndEdge(label_t src_label, const Any& src, label_t dst_label,
                        const Any& dst, label_t edge_label,
                        std::vector<Any>&& src_props,
                        std::vector<Any>&& dst_props, const Any& edge_prop);

  ~UpdateTransactionCRO();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  bool AddVertex(label_t label, const Any& oid, std::vector<Any>&& out);

  bool AddEdge(label_t src_label, const Any& src, label_t dst_label,
               const Any& dst, label_t edge_label, const Any& prop);

  bool GetVertexIndex(label_t label, const Any& id, vid_t& index) const;

  bool UpdateVertex(label_t label, const Any& oid, vid_t vid,
                    std::vector<Any>&& out);
  bool UpdateEdge(label_t src_label, const Any& src, label_t dst_label,
                  const Any& dst, label_t edge_label, const Any& prop,
                  vid_t src_lid, vid_t dst_lid,
                  std::shared_ptr<MutableCsrEdgeIterBase>& in_edge,
                  std::shared_ptr<MutableCsrEdgeIterBase>& out_edge);

 private:
  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  size_t get_csr_index(label_t src_label, label_t dst_label,
                       label_t edge_label) const;

  MutablePropertyFragment& graph_;
  MMapAllocator& alloc_;
  WalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;
  size_t vertex_label_num_;
  size_t edge_label_num_;
  std::vector<std::tuple<label_t, vid_t, std::vector<Any>>> update_vertices_;
  std::vector<std::tuple<label_t, Any, std::vector<Any>>> insert_vertices_;

  std::vector<std::tuple<std::shared_ptr<MutableCsrEdgeIterBase>,
                         std::shared_ptr<MutableCsrEdgeIterBase>, Any>>
      update_edges_;
  std::vector<std::tuple<label_t, Any, label_t, Any, label_t, Any>>
      insert_edges_;

  struct hash_pair {
    template <class T1, class T2>
    size_t operator()(const std::pair<T1, T2>& p) const {
      auto hash1 = GHash<T1>()(p.first);
      auto hash2 = GHash<T2>()(p.second);

      if (hash1 != hash2) {
        return hash1 ^ hash2;
      }

      // If hash1 == hash2, their XOR is zero.
      return hash1;
    }
  };

  std::vector<ska::flat_hash_map<vid_t, int>> update_vertices_map_;

  std::vector<ska::flat_hash_map<std::pair<vid_t, vid_t>, int, hash_pair>>
      update_edges_map_;
  std::vector<ska::flat_hash_map<Any, int, GHash<Any>>> insert_vertices_map_;
  std::vector<ska::flat_hash_map<std::pair<Any, Any>, int, hash_pair>>
      insert_edges_map_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_H_