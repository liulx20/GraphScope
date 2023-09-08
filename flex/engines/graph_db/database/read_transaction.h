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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <limits>
#include <utility>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

class AdjListViewBase {
 public:
  AdjListViewBase(std::shared_ptr<MutableCsrConstEdgeIterBase> iter,
                  timestamp_t timestamp)
      : iter_(iter), timestamp_(timestamp) {}
  class nbr_iterator {
   public:
    nbr_iterator(MutableCsrConstEdgeIterBase& iter, timestamp_t timestamp,
                 size_t cur)
        : iter_(iter), timestamp_(timestamp), cur_(cur), siz_(iter.size()) {}
    std::pair<Property, vid_t> operator*() {
      return {iter_.get_data(), iter_.get_neighbor()};
    }

    void next() {
      iter_.next();
      cur_++;
    }

    nbr_iterator& operator++() {
      next();
      while (iter_.is_valid() && iter_.get_timestamp() > timestamp_) {
        next();
      }
      return *this;
    }
    bool operator==(const nbr_iterator& rhs) const {
      return (cur_ == rhs.cur_);
    }
    bool operator!=(const nbr_iterator& rhs) const {
      return (cur_ != rhs.cur_);
    }
    MutableCsrConstEdgeIterBase& iter_;
    timestamp_t timestamp_;
    size_t cur_, siz_;
  };
  nbr_iterator begin() const { return nbr_iterator(*iter_, timestamp_, 0); }
  nbr_iterator end() const {
    return nbr_iterator(*iter_, timestamp_, iter_->size());
  }

 private:
  std::shared_ptr<MutableCsrConstEdgeIterBase> iter_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class ProjectedAdjListView {
 public:
  class nbr_iterator_base {
   public:
    nbr_iterator_base() = default;
    ~nbr_iterator_base() = default;
    virtual std::pair<EDATA_T, vid_t> operator*() = 0;
    virtual nbr_iterator_base& operator++() = 0;
    virtual bool operator==(const nbr_iterator_base& rhs) const = 0;
    virtual bool operator!=(const nbr_iterator_base& rhs) const = 0;
    virtual size_t estimated_degree() const = 0;
  };
  ProjectedAdjListView(const std::shared_ptr<nbr_iterator_base>& cur,
                       const std::shared_ptr<nbr_iterator_base>& end)
      : cur_(cur), end_(end) {}

  class simple_nbr_iterator : public nbr_iterator_base {
    using nbr_t = MutableNbr<EDATA_T>;

   public:
    simple_nbr_iterator(const nbr_t* ptr, const nbr_t* end,
                        timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_->timestamp > timestamp_ && ptr_ != end_) {
        ++ptr_;
      }
    }
    std::pair<EDATA_T, vid_t> operator*() override {
      return {ptr_->data, ptr_->neighbor};
    }
    size_t estimated_degree() const { return end_ - ptr_; }

    nbr_iterator_base& operator++() override {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator_base& rhs) const override {
      auto& _rhs = dynamic_cast<const simple_nbr_iterator&>(rhs);
      return (ptr_ == _rhs.ptr_);
    }

    bool operator!=(const nbr_iterator_base& rhs) const override {
      auto& _rhs = dynamic_cast<const simple_nbr_iterator&>(rhs);
      return (ptr_ != _rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
  };

  class column_nbr_iterator : public nbr_iterator_base {
    using nbr_t = MutableNbr<uint32_t>;

   public:
    column_nbr_iterator(const nbr_t* ptr, const nbr_t* end,
                        timestamp_t timestamp,
                        const TypedColumn<EDATA_T>& column)
        : ptr_(ptr), end_(end), timestamp_(timestamp), column_(column) {
      while (ptr_->timestamp > timestamp_ && ptr_ != end_) {
        ++ptr_;
      }
    }
    std::pair<EDATA_T, vid_t> operator*() override {
      return {column_.get_view(ptr_->data), ptr_->neighbor};
    }
    size_t estimated_degree() const { return end_ - ptr_; }
    nbr_iterator_base& operator++() override {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator_base& rhs) const override {
      auto& _rhs = dynamic_cast<const column_nbr_iterator&>(rhs);
      return (ptr_ == _rhs.ptr_);
    }

    bool operator!=(const nbr_iterator_base& rhs) const override {
      auto& _rhs = dynamic_cast<const column_nbr_iterator&>(rhs);
      return (ptr_ != _rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
    const TypedColumn<EDATA_T>& column_;
  };

  class nbr_iterator {
   public:
    nbr_iterator(nbr_iterator_base& iter) : iter_(iter) {}
    ~nbr_iterator() {}

    std::pair<EDATA_T, vid_t> operator*() { return iter_.operator*(); }

    nbr_iterator& operator++() {
      iter_.operator++();
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return iter_ == rhs.iter_;
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return iter_ != rhs.iter_;
    }

   private:
    nbr_iterator_base& iter_;
  };
  nbr_iterator begin() const { return nbr_iterator(*cur_); }
  nbr_iterator end() const { return nbr_iterator(*end_); }
  int estimated_degree() const { return cur_->estimated_degree(); }

 private:
  std::shared_ptr<nbr_iterator_base> cur_;
  std::shared_ptr<nbr_iterator_base> end_;
};

template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    using nbr_t = MutableNbr<EDATA_T>;

   public:
    nbr_iterator(const nbr_t* ptr, const nbr_t* end, timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_->timestamp > timestamp_ && ptr_ != end_) {
        ++ptr_;
      }
    }

    const nbr_t& operator*() const { return *ptr_; }

    const nbr_t* operator->() const { return ptr_; }

    nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

  int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};

class GraphViewBase {
 public:
  GraphViewBase(const MutableCsrBase& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
  AdjListViewBase get_edges(vid_t v) {
    // SingleGraphViewBase ?
    return AdjListViewBase(csr_.edge_iter(v), timestamp_);
  }

 private:
  const MutableCsrBase& csr_;
  timestamp_t timestamp_;
};
template <typename EDATA_T>
struct GetEdges {
  static ProjectedAdjListView<EDATA_T> get_edges(const MutableCsrBase& csr,
                                                 vid_t v, int col_id,
                                                 int timestamp) {
    if (csr.get_type() == CsrType::TYPED) {
      auto slices = dynamic_cast<const MutableCsr<EDATA_T>&>(csr).get_edges(v);
      std::shared_ptr<typename ProjectedAdjListView<EDATA_T>::nbr_iterator_base>
          cur(new typename ProjectedAdjListView<EDATA_T>::simple_nbr_iterator(
              slices.begin(), slices.end(), timestamp));
      std::shared_ptr<typename ProjectedAdjListView<EDATA_T>::nbr_iterator_base>
          end(new typename ProjectedAdjListView<EDATA_T>::simple_nbr_iterator(
              slices.end(), slices.end(), timestamp));

      return ProjectedAdjListView<EDATA_T>(cur, end);
    } else {
      const auto& tmp = dynamic_cast<const TableMutableCsr&>(csr);
      auto slices = tmp.get_edges(v);
      auto column = *(std::dynamic_pointer_cast<TypedColumn<EDATA_T>>(
          tmp.get_table().get_column_by_id(col_id)));
      std::shared_ptr<typename ProjectedAdjListView<EDATA_T>::nbr_iterator_base>
          cur(new typename ProjectedAdjListView<EDATA_T>::column_nbr_iterator(
              slices.begin(), slices.end(), timestamp, column));
      std::shared_ptr<typename ProjectedAdjListView<EDATA_T>::nbr_iterator_base>
          end(new typename ProjectedAdjListView<EDATA_T>::column_nbr_iterator(
              slices.end(), slices.end(), timestamp, column));

      return ProjectedAdjListView<EDATA_T>(cur, end);
    }
  }
};
template <>
struct GetEdges<std::string_view> {
  static ProjectedAdjListView<std::string_view> get_edges(
      const MutableCsrBase& csr, vid_t v, size_t col_id, int timestamp) {
    const auto& tmp = dynamic_cast<const StringMutableCsr&>(csr);
    auto slices = tmp.get_edges(v);
    const auto& column = tmp.get_column();
    std::shared_ptr<
        typename ProjectedAdjListView<std::string_view>::nbr_iterator_base>
        cur(new typename ProjectedAdjListView<
            std::string_view>::column_nbr_iterator(slices.begin(), slices.end(),
                                                   timestamp, column));
    std::shared_ptr<
        typename ProjectedAdjListView<std::string_view>::nbr_iterator_base>
        end(new typename ProjectedAdjListView<
            std::string_view>::column_nbr_iterator(slices.end(), slices.end(),
                                                   timestamp, column));
    return ProjectedAdjListView<std::string_view>(cur, end);
  }
};

template <typename EDATA_T>
class ProjectedGraphView {
 public:
  ProjectedGraphView(const MutableCsrBase& csr, timestamp_t timestamp,
                     size_t col_id = 0)
      : csr_(csr), timestamp_(timestamp), col_id_(col_id) {}

  ProjectedAdjListView<EDATA_T> get_edges(vid_t v) const {
    return GetEdges<EDATA_T>::get_edges(csr_, v, col_id_, timestamp_);
  }

 private:
  const MutableCsrBase& csr_;
  timestamp_t timestamp_;
  size_t col_id_;
};

template <typename EDATA_T>
class SingleProjectedGraphView {
 public:
  SingleProjectedGraphView(const MutableCsrBase& csr, timestamp_t timestamp,
                           size_t col_id, const TypedColumn<EDATA_T>& column)
      : csr_(csr), timestamp_(timestamp), col_id_(col_id), column_(column) {}

  bool exist(vid_t v) const {
    if (csr_.get_type() == CsrType::TYPED) {
      /**auto ts = dynamic_cast<const SingleMutableCsr<EDATA_T>&>(csr_)
                    .get_edge(v)
                    .timestamp.load();
      if (ts > timestamp_ && ts != 4294967295)
        printf("timestamp: %u %u\n", ts, timestamp_);*/
      return (dynamic_cast<const SingleMutableCsr<EDATA_T>&>(csr_)
                  .get_edge(v)
                  .timestamp.load() <= timestamp_);
    } else if (csr_.get_type() == CsrType::TABLE) {
      return (dynamic_cast<const SingleTableMutableCsr&>(csr_)
                  .get_edge(v)
                  .timestamp.load() <= timestamp_);
    } else {
      return (dynamic_cast<const SingleStringMutableCsr&>(csr_)
                  .get_edge(v)
                  .timestamp.load() <= timestamp_);
    }
  }

  std::pair<EDATA_T, vid_t> get_data(vid_t v) const {
    if (csr_.get_type() == CsrType::TYPED) {
      const auto& nbr =
          dynamic_cast<const SingleMutableCsr<EDATA_T>&>(csr_).get_edge(v);
      return {nbr.data, nbr.neighbor};
    } else if (csr_.get_type() == CsrType::TABLE) {
      const auto& nbr =
          dynamic_cast<const SingleTableMutableCsr&>(csr_).get_edge(v);
      return {column_.get_view(nbr.data), nbr.neighbor};
    } else {
      const auto& nbr =
          dynamic_cast<const SingleStringMutableCsr&>(csr_).get_edge(v);
      return {column_.get_view(nbr.data), nbr.neighbor};
    }
  }

 private:
  const MutableCsrBase& csr_;
  timestamp_t timestamp_;
  const TypedColumn<EDATA_T>& column_;
  size_t col_id_;
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_.get_edges(v), timestamp_);
  }

 private:
  const MutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class SingleGraphView {
 public:
  SingleGraphView(const SingleMutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  /**
  bool exist(vid_t v) const {
    bool flag = (csr_.get_edge(v).timestamp.load() <= timestamp_);
    if(!flag){
      flag = (csr_.get_edge(v).timestamp.load(std::memory_order_acquire) <=
  timestamp_);
    }
    return flag;
  }*/

  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleMutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

class TableGraphView {
 public:
  TableGraphView(const TableMutableCsr& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
  AdjListView<uint32_t> get_edges(vid_t v) const {
    return AdjListView<uint32_t>(csr_.get_edges(v), timestamp_);
  }

 private:
  const TableMutableCsr& csr_;
  timestamp_t timestamp_;
};

class SingleTableGraphView {
 public:
  SingleTableGraphView(const SingleTableMutableCsr& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }
  const MutableNbr<uint32_t>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleTableMutableCsr& csr_;
  timestamp_t timestamp_;
};

class ReadTransaction {
 public:
  ReadTransaction(const MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;

    Property GetField(int col_id) const;
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<MutableCsrConstEdgeIterBase> iter);
    ~edge_iterator();

    Property GetData() const;

    bool IsValid() const;

    void Next();

    Property GetField(int col_id) const;
    int FieldNum() const;
    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<MutableCsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, oid_t id) const;

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  AdjListView<uint32_t> GetTableOutgoingEdges(label_t v_label, vid_t v,
                                              label_t neighbor_label,
                                              label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<uint32_t, Property>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return AdjListView<uint32_t>(csr->get_edges(v), timestamp_);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  AdjListView<uint32_t> GetTableIncomingEdges(label_t v_label, vid_t v,
                                              label_t neighbor_label,
                                              label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<uint32_t, Property>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<uint32_t>(csr->get_edges(v), timestamp_);
  }

  const Schema& schema() const;

  GraphViewBase GetOutgoingGraphViewBase(label_t v_label,
                                         label_t neighbor_label,
                                         label_t edge_label) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    return GraphViewBase(*csr, timestamp_);
  }

  GraphViewBase GetIncomingGraphViewBase(label_t v_label,
                                         label_t neighbor_label,
                                         label_t edge_label) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    return GraphViewBase(*csr, timestamp_);
  }

  template <typename EDATA_T>
  ProjectedAdjListView<EDATA_T> GetIncomingProjectedEdges(
      label_t v_label, vid_t v, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    return GetEdges<EDATA_T>::get_edges(*csr, v, col_id, timestamp_);
  }

  template <typename EDATA_T>
  ProjectedAdjListView<EDATA_T> GetOutgoingProjectedEdges(
      label_t v_label, vid_t v, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    return GetEdges<EDATA_T>::get_edges(*csr, v, col_id, timestamp_);
  }

  template <typename EDATA_T>
  ProjectedGraphView<EDATA_T> GetOutgoingProjectedGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    return ProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id);
  }

  template <typename EDATA_T>
  ProjectedGraphView<EDATA_T> GetIncomingProjectedGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    return ProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id);
  }

  template <typename EDATA_T>
  SingleProjectedGraphView<EDATA_T> GetIncomingSingleProjectedGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::TYPED) {
      return SingleProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id,
                                               TypedColumn<EDATA_T>());
    } else {
      auto tmp = dynamic_cast<const TableMutableCsr*>(csr);
      const auto& column = *(std::dynamic_pointer_cast<TypedColumn<EDATA_T>>(
          tmp->get_table().get_column_by_id(col_id)));
      return SingleProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id,
                                               column);
    }
  }

  SingleProjectedGraphView<std::string_view>
  GetIncomingSingleProjectedGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label,
                                      size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    auto tmp = dynamic_cast<const StringMutableCsr*>(csr);
    return SingleProjectedGraphView<std::string_view>(*csr, timestamp_, col_id,
                                                      tmp->get_column());
  }

  template <typename EDATA_T>
  SingleProjectedGraphView<EDATA_T> GetOutgoingSingleProjectedGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::TYPED) {
      return SingleProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id,
                                               TypedColumn<EDATA_T>());
    } else {
      auto tmp = dynamic_cast<const TableMutableCsr*>(csr);
      const auto& column = *(std::dynamic_pointer_cast<TypedColumn<EDATA_T>>(
          tmp->get_table().get_column_by_id(col_id)));
      return SingleProjectedGraphView<EDATA_T>(*csr, timestamp_, col_id,
                                               column);
    }
  }

  SingleProjectedGraphView<std::string_view>
  GetOutgoingSingleProjectedGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label,
                                      size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    auto tmp = dynamic_cast<const StringMutableCsr*>(csr);
    return SingleProjectedGraphView<std::string_view>(*csr, timestamp_, col_id,
                                                      tmp->get_column());
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  TableGraphView GetOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    auto csr = dynamic_cast<const TableMutableCsr*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return TableGraphView(*csr, timestamp_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  TableGraphView GetIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    auto csr = dynamic_cast<const TableMutableCsr*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return TableGraphView(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  SingleTableGraphView GetOutgoingSingleGraphView(label_t v_label,
                                                  label_t neighbor_label,
                                                  label_t edge_label) const {
    auto csr = dynamic_cast<const SingleTableMutableCsr*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleTableGraphView(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  SingleTableGraphView GetIncomingSingleGraphView(label_t v_label,
                                                  label_t neighbor_label,
                                                  label_t edge_label) const {
    auto csr = dynamic_cast<const SingleTableMutableCsr*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleTableGraphView(*csr, timestamp_);
  }

 private:
  void release();

  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
