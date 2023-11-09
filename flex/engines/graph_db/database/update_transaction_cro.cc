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
static constexpr int commit_thread_num = 16;
static constexpr int multi_thread_limit = 256;
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
  vertex_label_num_ = graph_.schema().vertex_label_num();
  edge_label_num_ = graph_.schema().edge_label_num();
  update_vertices_map_.resize(vertex_label_num_);
  insert_vertices_map_.resize(vertex_label_num_);
  update_edges_map_.resize(vertex_label_num_ * vertex_label_num_ *
                           edge_label_num_);
  insert_edges_map_.resize(vertex_label_num_ * vertex_label_num_ *
                           edge_label_num_);
  update_edges_.reserve(4096);
  update_vertices_.reserve(4096);
  insert_edges_.reserve(4096);
  insert_vertices_.reserve(4096);
}

UpdateTransactionCRO::~UpdateTransactionCRO() { release(); }
timestamp_t UpdateTransactionCRO::timestamp() const { return timestamp_; }
void UpdateTransactionCRO::Abort() { release(); }

void UpdateTransactionCRO::Commit() {
  vm_.release_read_timestamp();
  timestamp_ = vm_.acquire_update_timestamp();
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

bool UpdateTransactionCRO::AddVertexAndEdge(label_t src_label, const Any& src,
                                            label_t dst_label, const Any& dst,
                                            label_t edge_label,
                                            std::vector<Any>&& src_props,
                                            std::vector<Any>&& dst_props,
                                            const Any& edge_prop) {
  vid_t src_lid, dst_lid;
  bool src_flag = false, dst_flag = false;
  if (graph_.get_lid(src_label, src, src_lid)) {
    UpdateVertex(src_label, src, src_lid, std::move(src_props));
    src_flag = true;
  } else {
    AddVertex(src_label, src, std::move(src_props));
  }
  if (graph_.get_lid(dst_label, dst, dst_lid)) {
    UpdateVertex(dst_label, dst, dst_lid, std::move(dst_props));
    dst_flag = true;
  } else {
    AddVertex(dst_label, dst, std::move(dst_props));
  }
  if (src_flag && dst_flag) {
    auto oes = graph_.get_outgoing_edges_mut(src_label, src_lid, dst_label,
                                             edge_label);
    std::shared_ptr<MutableCsrEdgeIterBase> in_ptr{nullptr}, out_ptr{nullptr};
    while (oes->is_valid()) {
      if (oes->get_neighbor() == dst_lid) {
        src_flag = false;
        out_ptr = oes;
        break;
      }
      oes->next();
    }
    auto ies = graph_.get_incoming_edges_mut(dst_label, dst_lid, src_label,
                                             edge_label);

    while (ies->is_valid()) {
      if (ies->get_neighbor() == src_lid) {
        dst_flag = false;
        in_ptr = ies;
        break;
      }
      ies->next();
    }
    if (!src_flag || !dst_flag) {
      UpdateEdge(src_label, src, dst_label, dst, edge_label, edge_prop, src_lid,
                 dst_lid, in_ptr, out_ptr);
      return true;
    }
  }
  AddEdge(src_label, src, dst_label, dst, edge_label, edge_prop);
  return true;
}

bool UpdateTransactionCRO::GetVertexIndex(label_t label, const Any& id,
                                          vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

bool UpdateTransactionCRO::AddVertex(label_t label, const Any& oid,
                                     std::vector<Any>&& props) {
  op_num_ += 1;
  grape::InArchive arc;
  for (auto& prop : props) {
    serialize_field(arc, prop);
  }
  if (insert_vertices_map_[label].find(oid) !=
      insert_vertices_map_[label].end()) {
    size_t ind = insert_vertices_map_[label].at(oid);
    std::get<2>(insert_vertices_[ind]) = std::move(props);
  } else {
    insert_vertices_map_[label][oid] = insert_vertices_.size();
    insert_vertices_.emplace_back(label, oid, std::move(props));
  }

  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, oid);
  arc_.AddBytes(arc.GetBuffer(), arc.GetSize());
  return true;
}

bool UpdateTransactionCRO::AddEdge(label_t src_label, const Any& src,
                                   label_t dst_label, const Any& dst,
                                   label_t edge_label, const Any& prop) {
  op_num_ += 1;
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  serialize_field(arc_, prop);
  size_t index = get_csr_index(src_label, dst_label, edge_label);
  if (insert_edges_map_[index].find({src, dst}) !=
      insert_edges_map_[index].end()) {
    size_t ind = insert_edges_map_[index][{src, dst}];
    std::get<5>(insert_edges_[ind]) = prop;
  } else {
    insert_edges_map_[index][{src, dst}] = insert_edges_.size();
    insert_edges_.emplace_back(src_label, src, dst_label, dst, edge_label,
                               prop);
  }
  return true;
}
bool UpdateTransactionCRO::UpdateVertex(label_t label, const Any& oid,
                                        vid_t vid, std::vector<Any>&& props) {
  op_num_ += 1;
  grape::InArchive arc;
  for (auto& prop : props) {
    serialize_field(arc, prop);
  }

  if (update_vertices_map_[label].find(vid) !=
      update_vertices_map_[label].end()) {
    size_t ind = update_vertices_map_[label].at(vid);
    std::get<2>(update_vertices_[ind]) = std::move(props);
  } else {
    update_vertices_map_[label][vid] = update_vertices_.size();
    update_vertices_.emplace_back(label, vid, std::move(props));
  }
  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, oid);
  arc_.AddBytes(arc.GetBuffer(), arc.GetSize());
  return true;
}

bool UpdateTransactionCRO::UpdateEdge(
    label_t src_label, const Any& src, label_t dst_label, const Any& dst,
    label_t edge_label, const Any& prop, vid_t src_lid, vid_t dst_lid,
    std::shared_ptr<MutableCsrEdgeIterBase>& in_edge,
    std::shared_ptr<MutableCsrEdgeIterBase>& out_edge) {
  op_num_ += 1;
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  serialize_field(arc_, prop);
  size_t index = get_csr_index(src_label, dst_label, edge_label);
  if (update_edges_map_[index].find({src_lid, dst_lid}) !=
      update_edges_map_[index].end()) {
    size_t ind = update_edges_map_[index][{src_lid, dst_lid}];
    std::get<2>(update_edges_[ind]) = prop;
  } else {
    update_edges_map_[index][{src_lid, dst_lid}] = update_edges_.size();
    update_edges_.emplace_back(in_edge, out_edge, prop);
  }

  return true;
}

size_t UpdateTransactionCRO::get_csr_index(label_t src_label, label_t dst_label,
                                           label_t edge_label) const {
  return src_label * vertex_label_num_ * edge_label_num_ +
         dst_label * edge_label_num_ + edge_label;
}

void UpdateTransactionCRO::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = std::numeric_limits<timestamp_t>::max();

    op_num_ = 0;
    insert_vertices_.clear();
    insert_edges_.clear();
    update_vertices_.clear();
    update_edges_.clear();
  }
}

void UpdateTransactionCRO::applyEdgesUpdates() {
  // int count = 0;
  {
    size_t num = insert_edges_.size();
    if (num < multi_thread_limit) {
      for (auto& [src_label, src, dst_label, dst, edge_label, prop] :
           insert_edges_) {
        grape::InArchive arc;
        arc << prop;
        vid_t src_lid, dst_lid;
        GetVertexIndex(src_label, src, src_lid);
        GetVertexIndex(dst_label, dst, dst_lid);
        grape::OutArchive out_arc(std::move(arc));
        graph_.IngestEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                          timestamp_, out_arc, alloc_);
        // count++;
      }
    } else {
      num /= commit_thread_num;
      std::vector<std::thread> vec;
      for (int i = 0; i < commit_thread_num; ++i) {
        vec.emplace_back(std::thread([&, i, num]() {
          for (size_t idx = i * num;
               idx < (i + 1) * num && idx < insert_edges_.size(); ++idx) {
            auto& [src_label, src, dst_label, dst, edge_label, prop] =
                insert_edges_[idx];
            grape::InArchive arc;
            arc << prop;
            vid_t src_lid, dst_lid;
            GetVertexIndex(src_label, src, src_lid);
            GetVertexIndex(dst_label, dst, dst_lid);
            grape::OutArchive out_arc(std::move(arc));
            graph_.IngestEdge(src_label, src_lid, dst_label, dst_lid,
                              edge_label, timestamp_, out_arc, alloc_);
          }
        }));
        for (auto& t : vec) {
          t.join();
        }
      }
    }
  }

  {
    size_t num = update_edges_.size();
    if (num < multi_thread_limit) {
      for (auto& [in_iter, out_iter, prop] : update_edges_) {
        if (in_iter != nullptr) {
          in_iter->set_data(prop, timestamp_);
        }
        if (out_iter != nullptr) {
          out_iter->set_data(prop, timestamp_);
        }
        // count++;
      }
    } else {
      num /= commit_thread_num;
      std::vector<std::thread> vec;
      for (int i = 0; i < commit_thread_num; ++i) {
        vec.emplace_back(std::thread([&, i, num]() {
          for (size_t idx = i * num;
               idx < (i + 1) * num && idx < update_edges_.size(); ++idx) {
            auto& [in_iter, out_iter, prop] = update_edges_[i];
            if (in_iter != nullptr) {
              in_iter->set_data(prop, timestamp_);
            }
            if (out_iter != nullptr) {
              out_iter->set_data(prop, timestamp_);
            }
          }
        }));
      }
      for (auto& t : vec) {
        t.join();
      }
    }
  }
  // LOG(INFO) << "Update Edge" << count << "\n";
}

void UpdateTransactionCRO::applyVerticesUpdates() {
  // int count = 0;
  {
    size_t num = insert_vertices_.size();
    if (num < multi_thread_limit) {
      for (auto& [label, oid, prop] : insert_vertices_) {
        vid_t lid = graph_.add_vertex(label, oid);
        graph_.get_vertex_table(label).insert(lid, prop);
        // count++;
      }
    } else {
      num /= commit_thread_num;
      std::vector<std::thread> vec;
      for (int i = 0; i < commit_thread_num; ++i) {
        vec.emplace_back([&, i, num]() {
          for (size_t idx = i * num;
               idx < (i + 1) * num && idx < insert_vertices_.size(); ++idx) {
            auto& [label, oid, prop] = insert_vertices_[idx];
            vid_t lid = graph_.add_vertex(label, oid);
            graph_.get_vertex_table(label).insert(lid, prop);
            // count++;
          }
        });
      }
      for (auto& t : vec) {
        t.join();
      }
    }
  }
  {
    size_t num = update_vertices_.size();
    if (num < multi_thread_limit) {
      for (auto& [label, vid, prop] : update_vertices_) {
        graph_.get_vertex_table(label).insert(vid, prop);
        // count++;
      }
    } else {
      num /= commit_thread_num;
      std::vector<std::thread> vec;
      for (int i = 0; i < commit_thread_num; ++i) {
        vec.emplace_back([&, i, num] {
          for (size_t idx = i * num;
               idx < (i + 1) * num && idx < update_vertices_.size(); ++idx) {
            auto& [label, vid, prop] = update_vertices_[idx];
            graph_.get_vertex_table(label).insert(vid, prop);
          }
        });
      }
      for (auto& t : vec) {
        t.join();
      }
    }
  }
  // LOG(INFO) << "Update Vertex" << count << "\n";
}

}  // namespace gs