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

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_

#include <atomic>
#include <type_traits>
#include <vector>

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"

namespace gs {

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  std::atomic<timestamp_t> timestamp;
  EDATA_T data;
};

template <>
struct MutableNbr<grape::EmptyType> {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  union {
    std::atomic<timestamp_t> timestamp;
    grape::EmptyType data;
  };
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const MutableNbr<std::string>& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              MutableNbr<std::string>& value);

template <typename EDATA_T>
class MutableNbrSlice {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSlice() = default;
  ~MutableNbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const nbr_t* ptr) { ptr_ = ptr; }

  const nbr_t* begin() const { return ptr_; }
  const nbr_t* end() const { return ptr_ + size_; }

  static MutableNbrSlice empty() {
    MutableNbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  const nbr_t* ptr_;
  int size_;
};

template <typename EDATA_T>
class MutableNbrSliceMut {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static MutableNbrSliceMut empty() {
    MutableNbrSliceMut ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};

template <typename T>
struct UninitializedUtils {
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    memcpy(new_buffer, old_buffer, len * sizeof(T));
  }
};

template <>
struct UninitializedUtils<MutableNbr<std::string>> {
  using T = MutableNbr<std::string>;
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    while (len--) {
      new_buffer->neighbor = old_buffer->neighbor;
      new_buffer->data = old_buffer->data;
      new_buffer->timestamp.store(old_buffer->timestamp);
      ++new_buffer;
      ++old_buffer;
    }
  }
};

template <typename EDATA_T, typename PROPERTY_T = EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;
  MutableAdjlist() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const PROPERTY_T& data,
                      timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(vid_t neighbor, const PROPERTY_T& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      auto* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut() {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};

template <>
class MutableAdjlist<std::string> {
 public:
  using nbr_t = MutableNbr<std::string>;
  using slice_t = MutableNbrSlice<std::string>;
  using mut_slice_t = MutableNbrSliceMut<std::string>;
  MutableAdjlist() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const std::string& data,
                      timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(vid_t neighbor, const std::string& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      auto* new_buffer = static_cast<nbr_t*>(allocator.allocate_typed(
          sizeof(nbr_t), capacity_,
          [](void* ptr) { static_cast<nbr_t*>(ptr)->~nbr_t(); }));
      for (int i = 0; i < capacity_; ++i) {
        new (&new_buffer[i]) nbr_t();
      }
      UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut() {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};

class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Property get_data() const = 0;
  virtual Property get_data(size_t index) const { return Property(); };
  virtual const int col_num() const { return 1; };
  virtual timestamp_t get_timestamp() const = 0;

  virtual size_t size() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Property get_data() const = 0;
  virtual Property get_data(size_t index) const { return Property(); };
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Property& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual void batch_init(vid_t vnum, const std::vector<int>& degree) = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                                timestamp_t ts, ArenaAllocator& alloc) = 0;

  virtual void Serialize(const std::string& path) = 0;

  virtual void Deserialize(const std::string& path) = 0;

  virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                           timestamp_t ts, ArenaAllocator& alloc) = 0;
  virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                                timestamp_t ts, ArenaAllocator& alloc) = 0;

  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const = 0;

  virtual MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;
};

template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Property get_data() const {
    Property data;
    data.set_value(cur_->data);
    return data;
  }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }
  size_t size() const { return end_ - cur_; }

 private:
  const nbr_t* cur_;
  const nbr_t* end_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Property get_data() const {
    Property ret;
    ret.set_value(cur_->data);
    return ret;
  }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void set_data(const Property& value, timestamp_t ts) {
    cur_->data = value.get_value<EDATA_T>();
    cur_->timestamp.store(ts);
  }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};

template <typename EDATA_T, typename PROPERTY_T = EDATA_T>
class TypedMutableCsrBase : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const PROPERTY_T& data,
                              timestamp_t ts = 0) = 0;

  virtual slice_t get_edges(vid_t i) const = 0;

  virtual void put_edge(vid_t src, vid_t dst, const PROPERTY_T& data,
                        timestamp_t ts, ArenaAllocator& alloc) = 0;
  virtual void set_table(Table* table) {}
  virtual void put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                   timestamp_t ts, ArenaAllocator& alloc) {}
  virtual void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                         timestamp_t ts = 0) {}
};

template <>
class TypedMutableCsrBase<uint32_t, std::string> : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<uint32_t>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const std::string& data,
                              timestamp_t ts = 0) = 0;

  virtual slice_t get_edges(vid_t i) const = 0;

  virtual void put_edge(vid_t src, vid_t dst, const std::string& data,
                        timestamp_t ts, ArenaAllocator& alloc) = 0;
  virtual void set_column(StringColumn* column) = 0;
  virtual void put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                   timestamp_t ts, ArenaAllocator& alloc) = 0;
  virtual void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                         timestamp_t ts = 0) = 0;
};

template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : adj_lists_(nullptr), locks_(nullptr), capacity_(0) {}
  ~MutableCsr() {
    if (adj_lists_ != nullptr) {
      free(adj_lists_);
    }
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    capacity_ = vnum + (vnum + 3) / 4;
    if (capacity_ == 0) {
      capacity_ = 1024;
    }

    adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
    locks_ = new grape::SpinLock[capacity_];
    size_t edge_capacity = 0;
    for (auto d : degree) {
      edge_capacity += (d + (d + 4) / 5);
    }
    init_nbr_list_.resize(edge_capacity);

    nbr_t* ptr = init_nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(ptr, cur_cap, 0);
      ptr += cur_cap;
    }
    for (vid_t i = vnum; i < capacity_; ++i) {
      adj_lists_[i].init(ptr, 0, 0);
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    put_edge(src, dst, data.get_value<EDATA_T>(), ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    CHECK_LT(src, capacity_);
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, allocator);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    Property value;
    arc >> value;
    put_edge(src, dst, value.get_value<EDATA_T>(), ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    Property value;
    arc.Peek<Property>(value);
    put_edge(src, dst, value.get_value<EDATA_T>(), ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  adjlist_t* adj_lists_;
  grape::SpinLock* locks_;
  vid_t capacity_;
  mmap_array<nbr_t> init_nbr_list_;
};

class StringMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
 public:
  explicit StringMutableCsrConstEdgeIter(const MutableNbrSlice<uint32_t>& slice,
                                         const StringColumn* column)
      : nbr_iter_(slice), column_(column) {}
  ~StringMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const { return nbr_iter_.get_neighbor(); }
  Property get_data() const {
    // TODO?
    Property prop;
    std::string_view sw =
        column_->get_view(nbr_iter_.get_data().get_value<uint32_t>());
    prop.set_value(std::string(sw.data(), sw.size()));
    return prop;
  }

  const StringColumn* get_column() const { return column_; }
  timestamp_t get_timestamp() const { return nbr_iter_.get_timestamp(); }

  void next() { nbr_iter_.next(); }
  bool is_valid() const { return nbr_iter_.is_valid(); }
  size_t size() const { return nbr_iter_.size(); }

 private:
  TypedMutableCsrConstEdgeIter<uint32_t> nbr_iter_;
  const StringColumn* column_;
};

class StringMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<uint32_t>;

 public:
  explicit StringMutableCsrEdgeIter(MutableNbrSliceMut<uint32_t> slice,
                                    StringColumn* column)
      : nbr_iter_(slice), column_(column) {}
  ~StringMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const { return nbr_iter_.get_neighbor(); }
  Property get_data() const {
    Property prop;
    std::string_view sw =
        column_->get_view(nbr_iter_.get_data().get_value<uint32_t>());
    prop.set_value(std::string(sw.data(), sw.size()));
    return prop;
  }

  const StringColumn* get_column() const { return column_; }
  timestamp_t get_timestamp() const { return nbr_iter_.get_timestamp(); }

  void set_data(const Property& value, timestamp_t ts) {
    column_->set(nbr_iter_.get_data().get_value<uint32_t>(), value);
    nbr_iter_.set_data(nbr_iter_.get_data(), ts);
  }

  void next() { nbr_iter_.next(); }
  bool is_valid() const { return nbr_iter_.is_valid(); }

 private:
  TypedMutableCsrEdgeIter<uint32_t> nbr_iter_;
  StringColumn* column_;
};

class StringMutableCsr : public TypedMutableCsrBase<unsigned, std::string> {
 public:
  using nbr_t = MutableNbr<uint32_t>;
  using adjlist_t = MutableAdjlist<uint32_t>;
  using slice_t = MutableNbrSlice<uint32_t>;
  using mut_slice_t = MutableNbrSliceMut<uint32_t>;
  StringMutableCsr() : column_ptr_(nullptr), topology_() {}
  ~StringMutableCsr() {}

  void set_column(StringColumn* col) { column_ptr_ = col; }
  StringColumn& get_column() { return *column_ptr_; }
  const StringColumn& get_column() const { return *column_ptr_; }

  void batch_init(vid_t vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                 timestamp_t ts) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(vid_t src, vid_t dst, const std::string& prop,
                      timestamp_t ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(vid_t src, vid_t dst, const std::string& prop, timestamp_t ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }

  int degree(vid_t i) const { return topology_.degree(i); }
  slice_t get_edges(vid_t i) const override { return topology_.get_edges(i); }

  mut_slice_t get_edges_mut(vid_t i) { return topology_.get_edges_mut(i); }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {}
  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {}
  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {}

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<StringMutableCsrConstEdgeIter>(
        topology_.get_edges(v), column_ptr_);
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new StringMutableCsrConstEdgeIter(topology_.get_edges(v),
                                             column_ptr_);
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<StringMutableCsrEdgeIter>(
        topology_.get_edges_mut(v), column_ptr_);
  }

  StringColumn* column_ptr_;
  MutableCsr<uint32_t> topology_;
};

template <>
class MutableCsr<std::string> : public TypedMutableCsrBase<std::string> {
 public:
  using nbr_t = MutableNbr<std::string>;
  using adjlist_t = MutableAdjlist<std::string>;
  using slice_t = MutableNbrSlice<std::string>;
  using mut_slice_t = MutableNbrSliceMut<std::string>;

  MutableCsr() : adj_lists_(nullptr), locks_(nullptr), capacity_(0) {}
  ~MutableCsr() {
    if (adj_lists_ != nullptr) {
      free(adj_lists_);
    }
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    capacity_ = vnum + (vnum + 3) / 4;
    if (capacity_ == 0) {
      capacity_ = 1024;
    }

    adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
    locks_ = new grape::SpinLock[capacity_];
    size_t edge_capacity = 0;
    for (auto d : degree) {
      edge_capacity += (d + (d + 4) / 5);
    }
    nbr_list_.resize(edge_capacity);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(ptr, cur_cap, 0);
      ptr += cur_cap;
    }
    for (vid_t i = vnum; i < capacity_; ++i) {
      adj_lists_[i].init(NULL, 0, 0);
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const std::string& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    put_edge(src, dst, data.get_value<std::string>(), ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const std::string& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    CHECK_LT(src, capacity_);
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, allocator);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    Property value;
    arc >> value;
    put_edge(src, dst, value.get_value<std::string>(), ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    Property value;
    arc.Peek<Property>(value);
    put_edge(src, dst, value.get_value<std::string>(), ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string>>(
        get_edges_mut(v));
  }

 private:
  adjlist_t* adj_lists_;
  std::vector<nbr_t> nbr_list_;
  grape::SpinLock* locks_;
  vid_t capacity_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    vid_t capacity = vnum + (vnum + 3) / 4;
    nbr_list_.resize(capacity);
    for (vid_t i = 0; i < capacity; ++i) {
      nbr_list_[i].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator&) override {
    put_edge(src, dst, data.get_value<EDATA_T>(), ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& alloc) override {
    put_edge(src, dst, data, ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    Property value;
    arc >> value;
    put_edge(src, dst, value.get_value<EDATA_T>(), ts);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    Property value;
    arc.Peek<Property>(value);
    put_edge(src, dst, value.get_value<EDATA_T>(), ts);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

class SingleStringMutableCsr
    : public TypedMutableCsrBase<unsigned, std::string> {
 public:
  using nbr_t = MutableNbr<uint32_t>;
  using slice_t = MutableNbrSlice<uint32_t>;
  using mut_slice_t = MutableNbrSliceMut<uint32_t>;
  SingleStringMutableCsr() : column_ptr_(nullptr), topology_() {}
  ~SingleStringMutableCsr() {}

  void set_column(StringColumn* col) { column_ptr_ = col; }
  StringColumn& get_column() { return *column_ptr_; }
  const StringColumn& get_column() const { return *column_ptr_; }

  void batch_init(vid_t vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                 timestamp_t ts) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(vid_t src, vid_t dst, const std::string& prop,
                      timestamp_t ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(vid_t src, vid_t dst, const std::string& prop, timestamp_t ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }
  slice_t get_edges(vid_t i) const override { return topology_.get_edges(i); }

  mut_slice_t get_edges_mut(vid_t i) { return topology_.get_edges_mut(i); }
  const nbr_t& get_edge(vid_t i) const { return topology_.get_edge(i); }
  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {}
  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {}
  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    // put_edge(src, dst, data, ts, alloc);
  }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<StringMutableCsrConstEdgeIter>(
        topology_.get_edges(v), column_ptr_);
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new StringMutableCsrConstEdgeIter(topology_.get_edges(v),
                                             column_ptr_);
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<StringMutableCsrEdgeIter>(
        topology_.get_edges_mut(v), column_ptr_);
  }

 private:
  StringColumn* column_ptr_;
  SingleMutableCsr<uint32_t> topology_;
};

template <>
class SingleMutableCsr<std::string> : public TypedMutableCsrBase<std::string> {
 public:
  using nbr_t = MutableNbr<std::string>;
  using slice_t = MutableNbrSlice<std::string>;
  using mut_slice_t = MutableNbrSliceMut<std::string>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    vid_t capacity = vnum + (vnum + 3) / 4;
    nbr_list_.resize(capacity);
    for (vid_t i = 0; i < capacity; ++i) {
      nbr_list_[i].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const std::string& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator&) override {
    put_edge(src, dst, data.get_value<std::string>(), ts);
  }

  void put_edge(vid_t src, vid_t dst, const std::string& data, timestamp_t ts) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_edge(vid_t src, vid_t dst, const std::string& data, timestamp_t ts,
                ArenaAllocator& alloc) override {
    put_edge(src, dst, data, ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    std::string value;
    arc >> value;
    put_edge(src, dst, value, ts);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    std::string value;
    arc.Peek<std::string>(value);
    put_edge(src, dst, value, ts);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string>>(
        get_edges_mut(v));
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

class TableMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
 public:
  explicit TableMutableCsrConstEdgeIter(const MutableNbrSlice<uint32_t>& slice,
                                        const Table* table)
      : nbr_iter_(slice), table_(table) {}
  ~TableMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const { return nbr_iter_.get_neighbor(); }
  Property get_data() const {
    return table_->get_row(nbr_iter_.get_data().get_value<uint32_t>());
  }

  Property get_data(size_t index) const {
    return table_->get_column_by_id(index)->get(
        nbr_iter_.get_data().get_value<uint32_t>());
  }
  const int col_num() const { return table_->col_num(); }
  timestamp_t get_timestamp() const { return nbr_iter_.get_timestamp(); }

  void next() { nbr_iter_.next(); }
  bool is_valid() const { return nbr_iter_.is_valid(); }
  size_t size() const { return nbr_iter_.size(); }

 private:
  TypedMutableCsrConstEdgeIter<uint32_t> nbr_iter_;
  const Table* table_;
};

class TableMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<uint32_t>;

 public:
  explicit TableMutableCsrEdgeIter(MutableNbrSliceMut<uint32_t> slice,
                                   Table* table)
      : nbr_iter_(slice), table_(table) {}
  ~TableMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const { return nbr_iter_.get_neighbor(); }
  Property get_data() const {
    return table_->get_row(nbr_iter_.get_data().get_value<uint32_t>());
  }

  Property get_data(size_t index) const {
    return table_->get_column_by_id(index)->get(
        nbr_iter_.get_data().get_value<uint32_t>());
  }

  const int col_num() const { return table_->col_num(); }
  timestamp_t get_timestamp() const { return nbr_iter_.get_timestamp(); }

  void set_data(const Property& value, timestamp_t ts) {
    table_->insert(nbr_iter_.get_data().get_value<uint32_t>(), value);
    nbr_iter_.set_data(nbr_iter_.get_data(), ts);
  }

  void next() { nbr_iter_.next(); }
  bool is_valid() const { return nbr_iter_.is_valid(); }

 private:
  TypedMutableCsrEdgeIter<uint32_t> nbr_iter_;
  Table* table_;
};

class TableMutableCsr : public TypedMutableCsrBase<uint32_t, Property> {
 public:
  using nbr_t = MutableNbr<uint32_t>;
  using adjlist_t = MutableAdjlist<uint32_t, Property>;
  using slice_t = MutableNbrSlice<uint32_t>;
  using mut_slice_t = MutableNbrSliceMut<uint32_t>;

  TableMutableCsr() {}
  ~TableMutableCsr() {}
  void set_table(Table* table) { table_ptr_ = table; }

  Table& get_table() { return *table_ptr_; }
  const Table& get_table() const { return *table_ptr_; }
  void batch_init(vid_t vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                 timestamp_t ts = 0) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(vid_t src, vid_t dst, const Property& prop,
                      timestamp_t ts = 0) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(vid_t src, vid_t dst, const Property& prop, timestamp_t ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }

  slice_t get_edges(vid_t i) const override { return topology_.get_edges(i); }

  mut_slice_t get_edges_mut(vid_t i) { return topology_.get_edges_mut(i); }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {}
  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {}
  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    put_edge(src, dst, data, ts, alloc);
  }

  int degree(vid_t i) const { return topology_.degree(i); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TableMutableCsrConstEdgeIter>(
        topology_.get_edges(v), table_ptr_);
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TableMutableCsrConstEdgeIter(topology_.get_edges(v), table_ptr_);
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TableMutableCsrEdgeIter>(topology_.get_edges_mut(v),
                                                     table_ptr_);
  }

 private:
  MutableCsr<uint32_t> topology_;
  Table* table_ptr_;
};

class SingleTableMutableCsr : public TypedMutableCsrBase<uint32_t, Property> {
 public:
  using nbr_t = MutableNbr<uint32_t>;
  using slice_t = MutableNbrSlice<uint32_t>;
  using mut_slice_t = MutableNbrSliceMut<uint32_t>;
  SingleTableMutableCsr() {}
  ~SingleTableMutableCsr() {}
  void set_table(Table* table) { table_ptr_ = table; }

  Table& get_table() { return *table_ptr_; }
  const Table& get_table() const { return *table_ptr_; }
  void batch_init(vid_t vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                 timestamp_t ts = 0) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(vid_t src, vid_t dst, const Property& prop,
                      timestamp_t ts = 0) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(vid_t src, vid_t dst, const Property& prop, timestamp_t ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }

  slice_t get_edges(vid_t i) const override { return topology_.get_edges(i); }

  mut_slice_t get_edges_mut(vid_t i) { return topology_.get_edges_mut(i); }

  const nbr_t& get_edge(vid_t i) const { return topology_.get_edge(i); }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {}
  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {}
  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    put_edge(src, dst, data, ts, alloc);
  }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TableMutableCsrConstEdgeIter>(
        topology_.get_edges(v), table_ptr_);
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TableMutableCsrConstEdgeIter(topology_.get_edges(v), table_ptr_);
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TableMutableCsrEdgeIter>(topology_.get_edges_mut(v),
                                                     table_ptr_);
  }

 private:
  SingleMutableCsr<uint32_t> topology_;
  Table* table_ptr_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
