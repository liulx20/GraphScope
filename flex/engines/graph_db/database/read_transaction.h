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

class GenericAdjListView {
 public:
  GenericAdjListView(std::shared_ptr<MutableCsrConstEdgeIterBase> iter,
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
class AdjListViewBase {
 public:
  class projected_nbr_iterator {
   public:
    projected_nbr_iterator() : cur_(nullptr), end_(nullptr), data_(nullptr) {}
    projected_nbr_iterator(const void* cur, const void* end, timestamp_t ts,
                           const TypedColumn<EDATA_T>* data = nullptr)
        : cur_(cur), end_(end), timestamp_(ts), data_(data) {
      if (data_ == nullptr) {
        auto ptr = static_cast<const MutableNbr<EDATA_T>*>(cur_);
        auto end = static_cast<const MutableNbr<EDATA_T>*>(end_);
        // printf("ptr: %p\n", ptr);
        while (ptr->timestamp > timestamp_ && ptr != end) {
          ++ptr;
        }
        cur_ = ptr;
      } else {
        auto ptr = static_cast<const MutableNbr<uint32_t>*>(cur_);
        auto end = static_cast<const MutableNbr<uint32_t>*>(end_);
        while (ptr->timestamp > timestamp_ && ptr != end) {
          ++ptr;
        }
        cur_ = ptr;
      }
    }

    size_t estimated_degree() const {
      if (data_ == nullptr) {
        auto end = static_cast<const MutableNbr<EDATA_T>*>(end_);
        auto cur = static_cast<const MutableNbr<EDATA_T>*>(cur_);
        return end - cur;
      } else {
        auto end = static_cast<const MutableNbr<uint32_t>*>(end_);
        auto cur = static_cast<const MutableNbr<uint32_t>*>(cur_);
        return end - cur;
      }
    }

    projected_nbr_iterator& operator++() {
      if (data_ == nullptr) {
        auto end = static_cast<const MutableNbr<EDATA_T>*>(end_);
        auto cur = static_cast<const MutableNbr<EDATA_T>*>(cur_);
        ++cur;
        while (cur != end && cur->timestamp > timestamp_) {
          ++cur;
        }
        cur_ = cur;
      } else {
        auto end = static_cast<const MutableNbr<uint32_t>*>(end_);
        auto cur = static_cast<const MutableNbr<uint32_t>*>(cur_);
        ++cur;
        while (cur != end && cur->timestamp > timestamp_) {
          ++cur;
        }
        cur_ = cur;
      }
      return *this;
    }

    vid_t get_neighbor() const {
      if (data_ == nullptr) {
        auto cur = static_cast<const MutableNbr<EDATA_T>*>(cur_);
        return cur->neighbor;
      } else {
        auto cur = static_cast<const MutableNbr<uint32_t>*>(cur_);
        return cur->neighbor;
      }
    }

    const EDATA_T& get_data() const {
      if (data_ == nullptr) {
        auto cur = static_cast<const MutableNbr<EDATA_T>*>(cur_);
        return cur->data;
      } else {
        auto cur = static_cast<const MutableNbr<uint32_t>*>(cur_);
        return (*data_)[cur->data];
      }
    }

    projected_nbr_iterator& operator*() { return *this; }

    bool operator==(const projected_nbr_iterator& rhs) const {
      return (cur_ == rhs.cur_);
    }

    bool operator!=(const projected_nbr_iterator& rhs) const {
      return (cur_ != rhs.cur_);
    }

   private:
    const void* cur_;
    const void* end_;
    const TypedColumn<EDATA_T>* data_;
    timestamp_t timestamp_;
  };
  AdjListViewBase()
      : cur_(projected_nbr_iterator()), end_(projected_nbr_iterator()) {}
  AdjListViewBase(const projected_nbr_iterator& cur,
                  const projected_nbr_iterator& end)
      : cur_(cur), end_(end) {}

  projected_nbr_iterator begin() const { return cur_; }
  projected_nbr_iterator end() const { return end_; }
  int estimated_degree() const { return cur_.estimated_degree(); }

 private:
  projected_nbr_iterator cur_;
  projected_nbr_iterator end_;
};

template <typename EDATA_T>
class SimpleAdjListView {
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

  SimpleAdjListView(const slice_t& slice, timestamp_t timestamp)
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

template <typename EDATA_T>
class ColumnAdjListView {
  class nbr_iterator {
    using nbr_t = MutableNbr<uint32_t>;

   public:
    nbr_iterator(const nbr_t* ptr, const nbr_t* end, timestamp_t timestamp,
                 const TypedColumn<EDATA_T>& column)
        : ptr_(ptr), end_(end), timestamp_(timestamp), column_(column) {
      while (ptr_->timestamp > timestamp_ && ptr_ != end_) {
        ++ptr_;
      }
    }

    const EDATA_T& get_data() const { return column_[ptr_->data]; }
    const vid_t get_neighbor() const { return ptr_->neighbor; }
    const nbr_iterator& operator*() const { return *this; }

    const nbr_iterator* operator->() const { return this; }

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
    const TypedColumn<EDATA_T>& column_;
  };

 public:
  using slice_t = MutableNbrSlice<uint32_t>;

  ColumnAdjListView(const slice_t& slice, timestamp_t timestamp,
                    const TypedColumn<EDATA_T>& column)
      : edges_(slice), timestamp_(timestamp), column_(column) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_, column_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_, column_);
  }

  int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
  const TypedColumn<EDATA_T>& column_;
};

class GenericGraphView {
 public:
  GenericGraphView(const MutableCsrBase& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
  GenericAdjListView get_edges(vid_t v) {
    // SingleGraphViewBase ?
    return GenericAdjListView(csr_.edge_iter(v), timestamp_);
  }

 private:
  const MutableCsrBase& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
struct GetEdges {
  static AdjListViewBase<EDATA_T> get_edges(vid_t v, int col_id, int timestamp,
                                            const CsrType& csr_type,
                                            const void* csr_ptr) {
    using nbr_iterator =
        typename AdjListViewBase<EDATA_T>::projected_nbr_iterator;
    if (csr_type == CsrType::MULTIPLE_TYPED) {
      auto slices =
          static_cast<const MutableCsr<EDATA_T>*>(csr_ptr)->get_edges(v);
      nbr_iterator cur(slices.begin(), slices.end(), timestamp);
      nbr_iterator end(slices.end(), slices.end(), timestamp);

      return AdjListViewBase<EDATA_T>(std::move(cur), std::move(end));
    } else if (csr_type == CsrType::SINGLE_TYPED) {
      auto slices =
          static_cast<const SingleMutableCsr<EDATA_T>*>(csr_ptr)->get_edges(v);
      nbr_iterator cur(slices.begin(), slices.end(), timestamp);
      nbr_iterator end(slices.end(), slices.end(), timestamp);

      return AdjListViewBase<EDATA_T>(std::move(cur), std::move(end));

    }

    else if (csr_type == CsrType::MULTIPLE_TABLE) {
      const auto& tmp = static_cast<const TableMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      auto column = (dynamic_cast<const TypedColumn<EDATA_T>*>(
          tmp->get_table()->get_column_by_id_raw(col_id)));
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

      return AdjListViewBase<EDATA_T>(std::move(cur), std::move(end));
    } else if (csr_type == CsrType::SINGLE_TABLE) {
      const auto& tmp = static_cast<const SingleTableMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      auto column = (dynamic_cast<const TypedColumn<EDATA_T>*>(
          tmp->get_table()->get_column_by_id_raw(col_id)));
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

      return AdjListViewBase<EDATA_T>(std::move(cur), std::move(end));
    }
    return AdjListViewBase<EDATA_T>();
  }
};

template <>
struct GetEdges<std::string_view> {
  static AdjListViewBase<std::string_view> get_edges(vid_t v, size_t col_id,
                                                     int timestamp,
                                                     const CsrType& csr_type,
                                                     const void* csr_ptr) {
    using nbr_iterator =
        typename AdjListViewBase<std::string_view>::projected_nbr_iterator;
    if (csr_type == CsrType::MULTIPLE_STRING) {
      const auto& tmp = static_cast<const StringMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      const auto column = tmp->get_column();
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

      return AdjListViewBase<std::string_view>(std::move(cur), std::move(end));
    } else if (csr_type == CsrType::SINGLE_STRING) {
      const auto& tmp = static_cast<const StringMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      const auto column = tmp->get_column();
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

    } else if (csr_type == CsrType::MULTIPLE_TABLE) {
      const auto& tmp = static_cast<const TableMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      const auto* column = dynamic_cast<const TypedColumn<std::string_view>*>(
          tmp->get_table()->get_column_by_id_raw(col_id));
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

      return AdjListViewBase<std::string_view>(std::move(cur), std::move(end));
    } else if (csr_type == CsrType::SINGLE_TABLE) {
      const auto& tmp = static_cast<const SingleTableMutableCsr*>(csr_ptr);
      auto slices = tmp->get_edges(v);
      const auto* column = dynamic_cast<const TypedColumn<std::string_view>*>(
          tmp->get_table()->get_column_by_id_raw(col_id));
      nbr_iterator cur(slices.begin(), slices.end(), timestamp, column);
      nbr_iterator end(slices.end(), slices.end(), timestamp, column);

      return AdjListViewBase<std::string_view>(std::move(cur), std::move(end));
    }
    return AdjListViewBase<std::string_view>();
  }
};

template <typename EDATA_T>
class GraphViewBase {
 public:
  GraphViewBase(const MutableCsrBase& csr, timestamp_t timestamp,
                size_t col_id = 0)
      : csr_(csr),
        csr_type_(csr_.get_type()),
        timestamp_(timestamp),
        col_id_(col_id) {
    init();
  }
  void init() {
    switch (csr_type_) {
    case CsrType::MULTIPLE_TYPED:
      csr_ptr_ = dynamic_cast<const MutableCsr<EDATA_T>*>(&csr_);
      break;
    case CsrType::SINGLE_TYPED:
      csr_ptr_ = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(&csr_);
      break;
    case CsrType::MULTIPLE_STRING:
      csr_ptr_ = dynamic_cast<const StringMutableCsr*>(&csr_);
      break;
    case CsrType::SINGLE_STRING:
      csr_ptr_ = dynamic_cast<const SingleStringMutableCsr*>(&csr_);
      break;

    case CsrType::MULTIPLE_TABLE:
      csr_ptr_ = dynamic_cast<const TableMutableCsr*>(&csr_);
      break;
    case CsrType::SINGLE_TABLE:
      csr_ptr_ = dynamic_cast<const SingleTableMutableCsr*>(&csr_);
      break;
    }
  }

  AdjListViewBase<EDATA_T> get_edges(vid_t v) const {
    return GetEdges<EDATA_T>::get_edges(v, col_id_, timestamp_, csr_type_,
                                        csr_ptr_);
  }

  bool exist(vid_t v) const {
    const auto& es = get_edges(v);
    return es.begin() != es.end();
  }
  typename AdjListViewBase<EDATA_T>::projected_nbr_iterator get_edge(
      vid_t v) const {
    return get_edges(v).begin();
  }

 protected:
  const MutableCsrBase& csr_;
  CsrType csr_type_;
  timestamp_t timestamp_;
  size_t col_id_;
  const void* csr_ptr_;
};

template <typename EDATA_T>
class SimpleGraphView : public GraphViewBase<EDATA_T> {
 public:
  SimpleGraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp,
                  size_t col_id = 0)
      : GraphViewBase<EDATA_T>(csr, timestamp, col_id), local_csr_(csr) {}

  SimpleAdjListView<EDATA_T> get_edges(vid_t v) const {
    return SimpleAdjListView<EDATA_T>(local_csr_.get_edges(v),
                                      GraphViewBase<EDATA_T>::timestamp_);
  }

 private:
  const MutableCsr<EDATA_T>& local_csr_;
};

template <typename EDATA_T>
class SimpleSingleGraphView : public GraphViewBase<EDATA_T> {
 public:
  SimpleSingleGraphView(const SingleMutableCsr<EDATA_T>& csr,
                        timestamp_t timestamp, size_t col_id = 0)
      : GraphViewBase<EDATA_T>(csr, timestamp, col_id), local_csr_(csr) {}

  bool exist(vid_t v) const {
    return (local_csr_.get_edge(v).timestamp.load() <=
            GraphViewBase<EDATA_T>::timestamp_);
  }

  const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return local_csr_.get_edge(v);
  }

 private:
  const SingleMutableCsr<EDATA_T>& local_csr_;
};

template <typename EDATA_T>
class ColumnGraphView : public GraphViewBase<EDATA_T> {
 public:
  ColumnGraphView(const TableMutableCsr& csr, timestamp_t timestamp,
                  size_t col_id = 0)
      : GraphViewBase<EDATA_T>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*(dynamic_cast<const TypedColumn<EDATA_T>*>(
            csr.get_table()->get_column_by_id_raw(col_id)))) {}

  ColumnAdjListView<EDATA_T> get_edges(vid_t v) const {
    return ColumnAdjListView<EDATA_T>(
        local_csr_.get_edges(v), GraphViewBase<EDATA_T>::timestamp_, column_);
  }

 private:
  const MutableCsr<uint32_t>& local_csr_;
  const TypedColumn<EDATA_T>& column_;
};

template <>
class ColumnGraphView<std::string_view>
    : public GraphViewBase<std::string_view> {
 public:
  ColumnGraphView(const TableMutableCsr& csr, timestamp_t timestamp,
                  size_t col_id = 0)
      : GraphViewBase<std::string_view>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*(dynamic_cast<const TypedColumn<std::string_view>*>(
            csr.get_table()->get_column_by_id_raw(col_id)))) {}
  ColumnGraphView(const StringMutableCsr& csr, timestamp_t timestamp,
                  size_t col_id = 0)
      : GraphViewBase<std::string_view>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*csr.get_column()) {}
  ColumnAdjListView<std::string_view> get_edges(vid_t v) const {
    return ColumnAdjListView<std::string_view>(
        local_csr_.get_edges(v), GraphViewBase<std::string_view>::timestamp_,
        column_);
  }

 private:
  const MutableCsr<uint32_t>& local_csr_;
  const TypedColumn<std::string_view>& column_;
};

template <typename EDATA_T>
class ColumnSingleGraphView : public GraphViewBase<EDATA_T> {
 public:
  ColumnSingleGraphView(const SingleTableMutableCsr& csr, timestamp_t timestamp,
                        size_t col_id = 0)
      : GraphViewBase<EDATA_T>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*(dynamic_cast<const TypedColumn<EDATA_T>*>(
            csr.get_table()->get_column_by_id_raw(col_id)))) {}
  bool exist(vid_t v) const {
    return (local_csr_.get_edge(v).timestamp.load() <=
            GraphViewBase<EDATA_T>::timestamp_);
  }
  vid_t get_neighbor() const { return nbr_->neighbor; }
  const EDATA_T& get_data() const { return column_[nbr_->data]; }
  const ColumnSingleGraphView<EDATA_T>& get_edge(vid_t v) const {
    nbr_ = &(local_csr_.get_edge(v));
    return *this;
  }

 private:
  MutableNbr<uint32_t>* nbr_;
  const SingleMutableCsr<uint32_t>& local_csr_;
  const TypedColumn<EDATA_T>& column_;
};

template <>
class ColumnSingleGraphView<std::string_view>
    : public GraphViewBase<std::string_view> {
 public:
  ColumnSingleGraphView(const SingleTableMutableCsr& csr, timestamp_t timestamp,
                        size_t col_id = 0)
      : GraphViewBase<std::string_view>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*(dynamic_cast<const TypedColumn<std::string_view>*>(
            csr.get_table()->get_column_by_id_raw(col_id)))) {}

  ColumnSingleGraphView(const SingleStringMutableCsr& csr,
                        timestamp_t timestamp, size_t col_id = 0)
      : GraphViewBase<std::string_view>(csr, timestamp, col_id),
        local_csr_(csr.get_index_csr()),
        column_(*csr.get_column()) {}
  bool exist(vid_t v) const {
    return (local_csr_.get_edge(v).timestamp.load() <= timestamp_);
  }
  const MutableNbr<uint32_t>& get_edge(vid_t v) const {
    return local_csr_.get_edge(v);
  }

 private:
  const SingleMutableCsr<uint32_t>& local_csr_;
  const TypedColumn<std::string_view>& column_;
};

template <typename EDATA_T>
struct GetGraphView {
  static std::shared_ptr<GraphViewBase<EDATA_T>> get_graph_view(
      const MutableCsrBase* csr, timestamp_t timestamp, size_t col_id) {
    switch (csr->get_type()) {
    case CsrType::MULTIPLE_TYPED: {
      auto tmp = dynamic_cast<const MutableCsr<EDATA_T>*>(csr);
      return std::shared_ptr<GraphViewBase<EDATA_T>>(
          new SimpleGraphView<EDATA_T>(*tmp, timestamp, col_id));
    }
    case CsrType::SINGLE_TYPED: {
      auto tmp = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(csr);
      return std::shared_ptr<GraphViewBase<EDATA_T>>(
          new SimpleSingleGraphView<EDATA_T>(*tmp, timestamp, col_id));
    }
    case CsrType::MULTIPLE_TABLE: {
      auto tmp = dynamic_cast<const TableMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<EDATA_T>>(
          new ColumnGraphView<EDATA_T>(*tmp, timestamp, col_id));
    }
    case CsrType::SINGLE_TABLE: {
      auto tmp = dynamic_cast<const SingleTableMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<EDATA_T>>(
          new ColumnSingleGraphView<EDATA_T>(*tmp, timestamp, col_id));
    }
    }
    return nullptr;
  }
};

template <>
struct GetGraphView<std::string_view> {
  static std::shared_ptr<GraphViewBase<std::string_view>> get_graph_view(
      const MutableCsrBase* csr, timestamp_t timestamp, size_t col_id) {
    switch (csr->get_type()) {
    case CsrType::MULTIPLE_STRING: {
      auto tmp = dynamic_cast<const StringMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<std::string_view>>(
          new ColumnGraphView<std::string_view>(*tmp, timestamp, col_id));
    }
    case CsrType::SINGLE_STRING: {
      auto tmp = dynamic_cast<const SingleStringMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<std::string_view>>(
          new ColumnSingleGraphView<std::string_view>(*tmp, timestamp, col_id));
    }
    case CsrType::MULTIPLE_TABLE: {
      auto tmp = dynamic_cast<const TableMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<std::string_view>>(
          new ColumnGraphView<std::string_view>(*tmp, timestamp, col_id));
    }
    case CsrType::SINGLE_TABLE: {
      auto tmp = dynamic_cast<const SingleTableMutableCsr*>(csr);
      return std::shared_ptr<GraphViewBase<std::string_view>>(
          new ColumnSingleGraphView<std::string_view>(*tmp, timestamp, col_id));
    }
    }
    return nullptr;
  }
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const std::shared_ptr<GraphViewBase<EDATA_T>>& ptr)
      : base_(*ptr), ptr_(ptr) {}
  AdjListViewBase<EDATA_T> get_edges(vid_t v) const {
    return base_.get_edges(v);
  }

  bool exist(vid_t v) const { return base_.exist(v); }
  typename AdjListViewBase<EDATA_T>::projected_nbr_iterator get_edge(
      vid_t v) const {
    return base_.get_edge(v);
  }

  GraphViewBase<EDATA_T>& base_;
  std::shared_ptr<GraphViewBase<EDATA_T>> ptr_;
};

template <typename GRAPHVIEW_T>
class GraphViewType {};

template <typename EDATA_T>
class GraphViewType<SimpleGraphView<EDATA_T>> {
 public:
  GraphViewType<SimpleGraphView<EDATA_T>>(GraphView<EDATA_T>&& gw)
      : ptr(std::move(gw.ptr_)),
        graph_(static_cast<SimpleGraphView<EDATA_T>&>(gw.base_)) {}
  SimpleAdjListView<EDATA_T> get_edges(vid_t v) const {
    return graph_.get_edges(v);
  }

 private:
  SimpleGraphView<EDATA_T>& graph_;
  std::shared_ptr<GraphViewBase<EDATA_T>> ptr;
};

template <typename EDATA_T>
class GraphViewType<ColumnGraphView<EDATA_T>> {
 public:
  GraphViewType<ColumnGraphView<EDATA_T>>(GraphView<EDATA_T>&& gw)
      : ptr(std::move(gw.ptr_)),
        graph_(static_cast<ColumnGraphView<EDATA_T>&>(gw.base_)) {}
  ColumnAdjListView<EDATA_T> get_edges(vid_t v) const {
    return graph_.get_edges(v);
  }

 private:
  ColumnGraphView<EDATA_T>& graph_;
  std::shared_ptr<GraphViewBase<EDATA_T>> ptr;
};
template <typename EDATA_T>
class GraphViewType<SimpleSingleGraphView<EDATA_T>> {
 public:
  GraphViewType<SimpleSingleGraphView<EDATA_T>>(GraphView<EDATA_T>&& gw)
      : ptr(std::move(gw.ptr_)),
        graph_(static_cast<SimpleSingleGraphView<EDATA_T>&>(gw.base_)) {}
  MutableNbr<EDATA_T> get_edge(vid_t v) const { return graph_.get_edge(v); }
  bool exist(vid_t v) const { return graph_.exist(v); }

 private:
  SimpleSingleGraphView<EDATA_T>& graph_;
  std::shared_ptr<GraphViewBase<EDATA_T>> ptr;
};

template <typename EDATA_T>
class GraphViewType<ColumnSingleGraphView<EDATA_T>> {
 public:
  GraphViewType<ColumnSingleGraphView<EDATA_T>>(GraphView<EDATA_T>&& gw)
      : ptr(std::move(gw.ptr_)),
        graph_(static_cast<ColumnSingleGraphView<EDATA_T>&>(gw.base_)) {}
  ColumnSingleGraphView<EDATA_T>& get_edge(vid_t v) const {
    return graph_.get_edge(v);
  }
  bool exist(vid_t v) const { return graph_.exist(v); }

 private:
  ColumnSingleGraphView<EDATA_T>& graph_;
  std::shared_ptr<GraphViewBase<EDATA_T>> ptr;
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

  const Schema& schema() const;

  GenericGraphView GetOutgoingGenericGraphView(label_t v_label,
                                               label_t neighbor_label,
                                               label_t edge_label) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    return GenericGraphView(*csr, timestamp_);
  }

  GenericGraphView GetIncomingGenericGraphView(label_t v_label,
                                               label_t neighbor_label,
                                               label_t edge_label) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    return GenericGraphView(*csr, timestamp_);
  }

  template <typename EDATA_T>
  AdjListViewBase<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                            label_t neighbor_label,
                                            label_t edge_label,
                                            size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::MULTIPLE_TYPED) {
      auto csr_ptr = dynamic_cast<const MutableCsr<EDATA_T>*>(csr);
      return GetEdges<EDATA_T>::get_edges(v, col_id, timestamp_,
                                          CsrType::MULTIPLE_TYPED, csr);
    } else {
      auto csr_ptr = dynamic_cast<const TableMutableCsr*>(csr);
      return GetEdges<EDATA_T>::get_edges(v, col_id, timestamp_,
                                          CsrType::MULTIPLE_TABLE, csr);
    }
  }

  AdjListViewBase<std::string_view> GetIncomingEdges(label_t v_label, vid_t v,
                                                     label_t neighbor_label,
                                                     label_t edge_label,
                                                     size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::MULTIPLE_STRING) {
      auto csr_ptr = dynamic_cast<const StringMutableCsr*>(csr);
      return GetEdges<std::string_view>::get_edges(
          v, col_id, timestamp_, CsrType::MULTIPLE_TYPED, csr);
    } else {
      auto csr_ptr = dynamic_cast<const TableMutableCsr*>(csr);
      return GetEdges<std::string_view>::get_edges(
          v, col_id, timestamp_, CsrType::MULTIPLE_TABLE, csr);
    }
  }

  template <typename EDATA_T>
  AdjListViewBase<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                            label_t neighbor_label,
                                            label_t edge_label,
                                            size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::MULTIPLE_TYPED) {
      auto csr_ptr = dynamic_cast<const MutableCsr<EDATA_T>*>(csr);
      return GetEdges<EDATA_T>::get_edges(v, col_id, timestamp_,
                                          CsrType::MULTIPLE_TYPED, csr);
    } else {
      auto csr_ptr = dynamic_cast<const TableMutableCsr*>(csr);
      return GetEdges<EDATA_T>::get_edges(v, col_id, timestamp_,
                                          CsrType::MULTIPLE_TABLE, csr);
    }
  }

  AdjListViewBase<std::string_view> GetOutgoingEdges(label_t v_label, vid_t v,
                                                     label_t neighbor_label,
                                                     label_t edge_label,
                                                     size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    if (csr->get_type() == CsrType::MULTIPLE_STRING) {
      auto csr_ptr = dynamic_cast<const StringMutableCsr*>(csr);
      return GetEdges<std::string_view>::get_edges(
          v, col_id, timestamp_, CsrType::MULTIPLE_TYPED, csr);
    } else {
      auto csr_ptr = dynamic_cast<const TableMutableCsr*>(csr);
      return GetEdges<std::string_view>::get_edges(
          v, col_id, timestamp_, CsrType::MULTIPLE_TABLE, csr);
    }
  }

  template <typename EDATA_T>
  /*std::shared_ptr<GraphViewBase<EDATA_T>>*/
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label,
                                          size_t col_id = 0) const {
    auto csr = graph_.get_oe_csr(v_label, neighbor_label, edge_label);
    return GetGraphView<EDATA_T>::get_graph_view(csr, timestamp_, col_id);
  }

  template <typename EDATA_T>
  /*std::shared_ptr<GraphViewBase<EDATA_T>>*/
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label,
                                          size_t col_id = 0) const {
    auto csr = graph_.get_ie_csr(v_label, neighbor_label, edge_label);

    return GetGraphView<EDATA_T>::get_graph_view(csr, timestamp_, col_id);
  }

 private:
  void release();

  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
