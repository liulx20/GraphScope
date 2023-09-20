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

template <typename EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;
  MutableAdjlist() : offset_(0), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(size_t offset, int cap, int size) {
    offset_ = offset;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(mmap_array<nbr_t>& nbr_list, vid_t neighbor,
                      const EDATA_T& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = nbr_list[offset_ + size_];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
    ++size_;
  }

  void put_edge(mmap_array<nbr_t>& nbr_list, std::atomic<size_t>& size,
                vid_t neighbor, const EDATA_T& data, timestamp_t ts) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      size_t new_offset = size.fetch_add(capacity_);
      UninitializedUtils<nbr_t>::copy(nbr_list.data() + new_offset,
                                      nbr_list.data() + offset_, size_);
      offset_ = new_offset;
    }
    auto& nbr = nbr_list[offset_ + size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges(const mmap_array<nbr_t>& nbr_list) const {
    slice_t ret;
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(nbr_list.data() + offset_);
    return ret;
  }

  mut_slice_t get_edges_mut(mmap_array<nbr_t>& nbr_list) {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(nbr_list.data() + offset_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data(const mmap_array<nbr_t>& nbr_list) const {
    return nbr_list.data() + offset_;
  }
  nbr_t* data(mmap_array<nbr_t>& nbr_list) { return nbr_list.data() + offset_; }

 private:
  size_t offset_;
  std::atomic<int> size_;
  int capacity_;
};

class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
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
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual void batch_init(const std::string& filename, vid_t vnum,
                          const std::vector<int>& degree) = 0;

  virtual void open(const std::string& filename) = 0;

  virtual void resize(vid_t vnum) = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                                timestamp_t ts) = 0;

  virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                           timestamp_t ts) = 0;
  virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                                timestamp_t ts) = 0;

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
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
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
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void set_data(const Any& value, timestamp_t ts) {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp.store(ts);
  }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};

template <typename EDATA_T>
class TypedMutableCsrBase : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;

  virtual slice_t get_edges(vid_t i) const = 0;
};

template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
    nbr_list_.resize(nbr_list_size_.load());
  }

  void batch_init(const std::string& prefix, vid_t vnum,
                  const std::vector<int>& degree) override {
    size_t vertex_capacity = vnum + (vnum + 3) / 4;
    if (vertex_capacity == 0) {
      vertex_capacity = 1024;
    }
    adj_lists_.open(prefix + ".adj");
    adj_lists_.resize(vertex_capacity);

    locks_ = new grape::SpinLock[vertex_capacity];

    size_t edge_capacity = 0;
    for (auto d : degree) {
      edge_capacity += (d + (d + 4) / 5);
    }
    nbr_list_size_.store(edge_capacity);
    edge_capacity += (edge_capacity >> 2) + 1;
    nbr_list_.open(prefix + ".nbr");
    nbr_list_.resize(edge_capacity);
    nbr_list_capacity_ = edge_capacity;

    size_t offset = 0;
    for (vid_t i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(offset, cur_cap, 0);
      offset += cur_cap;
    }
    for (vid_t i = vnum; i < vertex_capacity; ++i) {
      adj_lists_[i].init(offset, 0, 0);
    }
  }

  void open(const std::string& prefix) override {
    adj_lists_.open(prefix + ".adj");
    size_t size = adj_lists_.size();
    size_t capacity = size;
    capacity += (capacity >> 2);
    adj_lists_.resize(capacity);

    locks_ = new grape::SpinLock[capacity];
    nbr_list_.open(prefix + ".nbr");
    nbr_list_size_.store(nbr_list_.size());
    nbr_list_capacity_ = nbr_list_size_.load();
    nbr_list_capacity_ += (nbr_list_capacity_ >> 2);
    nbr_list_.resize(nbr_list_capacity_);

    for (size_t k = size; k != capacity; ++k) {
      adj_lists_[k].init(nbr_list_size_.load(), 0, 0);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(nbr_list_size_.load(), 0, 0);
      }

      size_t edge_capacity = nbr_list_size_.load();
      edge_capacity += (edge_capacity >> 2);

      if (edge_capacity > nbr_list_capacity_) {
        nbr_list_.resize(edge_capacity);
        nbr_list_capacity_ = edge_capacity;
      }
    } else {
      adj_lists_.resize(vnum);
      nbr_list_.resize(nbr_list_size_.load());
      nbr_list_capacity_ = nbr_list_size_.load();
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(nbr_list_, dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                        timestamp_t ts) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(nbr_list_, nbr_list_size_, dst, data, ts);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges(nbr_list_);
  }
  mut_slice_t get_edges_mut(vid_t i) {
    return adj_lists_[i].get_edges_mut(nbr_list_);
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                   timestamp_t ts) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts);
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
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
  std::atomic<size_t> nbr_list_size_;
  size_t nbr_list_capacity_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(const std::string& prefix, vid_t vnum,
                  const std::vector<int>& degree) override {
    vid_t capacity = vnum + (vnum + 3) / 4;
    nbr_list_.open(prefix + ".nbr");
    nbr_list_.resize(capacity);
    for (vid_t i = 0; i < capacity; ++i) {
      nbr_list_[i].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }

  void open(const std::string& prefix) override {
    nbr_list_.open(prefix + ".nbr");
    size_t size = nbr_list_.size();
    size_t capacity = size;
    capacity += (capacity >> 2);

    nbr_list_.resize(capacity);
    for (size_t k = size; k != capacity; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
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

  void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                        timestamp_t ts) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
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

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                   timestamp_t ts) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts);
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

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(const std::string& filename, vid_t vnum,
                  const std::vector<int>& degree) override {}

  void open(const std::string& prefix) override {}

  void resize(vid_t vnum) override {}

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                        timestamp_t ts) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                   timestamp_t ts) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
};
}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
