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

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/update_transaction_cro.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {
UpdateTransactionCRO::UpdateTransactionCRO(MutablePropertyFragment& graph,
                                           MMapAllocator& alloc,
                                           WalWriter& logger,
                                           VersionManager& vm,
                                           timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));
}

UpdateTransactionCRO::~UpdateTransactionCRO() { release(); }
timestamp_t UpdateTransactionCRO::timestamp() const { return timestamp_; }
void UpdateTransactionCRO::Abort() { release(); }

void UpdateTransactionCRO::Commit() {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return;
  }
  if (op_num_ == 0) {
    release();
    return;
  }

  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 1;
  header->timestamp = timestamp_;
  logger_.append(arc_.GetBuffer(), arc_.GetSize());

  applyVerticesUpdates();
  applyEdgesUpdates();
  release();
}

bool UpdateTransactionCRO::UpdateVertex(label_t label, const Any& oid,
                                        vid_t vid, std::vector<Any>&& props) {
  op_num_ += 1;
  grape::InArchive arc;
  for (auto& prop : props) {
    serialize_field(arc, prop);
  }
  update_verties_.emplace_back(label, vid, std::move(props));

  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, oid);
  arc_.AddBytes(arc.GetBuffer(), arc.GetSize());
  return true;
}

bool UpdateTransactionCRO::UpdateEdge(
    label_t src_label, const Any& src, label_t dst_label, const Any& dst,
    label_t edge_label, const Any& prop,
    std::shared_ptr<MutableCsrEdgeIterBase>& in_edge,
    std::shared_ptr<MutableCsrEdgeIterBase>& out_edge) {
  op_num_ += 1;
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  serialize_field(arc_, prop);
  update_edges_.emplace_back(in_edge, out_edge, prop);
  return true;
}

void UpdateTransactionCRO::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = std::numeric_limits<timestamp_t>::max();

    op_num_ = 0;
    update_verties_.clear();
    update_edges_.clear();
  }
}

void UpdateTransactionCRO::applyEdgesUpdates() {
  for (auto& [in_iter, out_iter, prop] : update_edges_) {
    if (in_iter != nullptr) {
      in_iter->set_data(prop, timestamp_);
    }
    if (out_iter != nullptr) {
      out_iter->set_data(prop, timestamp_);
    }
  }
}

void UpdateTransactionCRO::applyVerticesUpdates() {
  for (auto& [label, vid, prop] : update_verties_) {
    graph_.get_vertex_table(label).insert(vid, prop);
  }
}

}  // namespace gs