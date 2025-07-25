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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VERTEX_COLUMNS_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VERTEX_COLUMNS_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/i_context_column.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/utils/configs.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
// using gs::vid_t;
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

namespace chunked_runtime {
using gs::vid_t;
using gs::runtime::VertexRecord;

enum class VertexColumnType {
  kSingle,
  kMultiSegment,
  kMultiple,
};

class IVertexColumn : public IContextColumn {
 public:
  IVertexColumn() = default;
  virtual ~IVertexColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertex;
  }

  virtual VertexColumnType vertex_column_type() const = 0;
  virtual VertexRecord get_vertex(size_t idx) const = 0;

  RTAny get_elem(size_t idx) const override {
    return RTAny::from_vertex(this->get_vertex(idx));
  }

  RTAnyType elem_type() const override { return RTAnyType::kVertex; }

  virtual std::unordered_set<label_t> get_labels_set() const = 0;

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const;

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func,
                      const ValueColumn<size_t>& offsets) const;
};

class SLVertexColumn : public IVertexColumn {
 public:
  SLVertexColumn(const LocalMemPool& mem_pool, label_t label)
      : is_optional_(false),
        label_(label),
        size_(0),
        data_(static_cast<vid_t*>(
            mem_pool.Allocate(sizeof(vid_t) * Configs::CHUNK_SIZE))),
        local_pool_(mem_pool) {}

  ~SLVertexColumn() {
    if (data_) {
      local_pool_.Deallocate(data_, sizeof(vid_t) * Configs::CHUNK_SIZE);
      data_ = nullptr;
    }
  }

  void clear() override { size_ = 0; }

  size_t size() const override { return size_; }

  void push_back_opt(vid_t value) { data_[size_++] = value; }

  void emplace_back(VertexRecord&& value) { data_[size_++] = value.vid_; }

  void push_back(const VertexRecord& value) { data_[size_++] = value.vid_; }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift);

  void push_back_null() {
    is_optional_ = true;
    data_[size_++] = std::numeric_limits<vid_t>::max();
  }

  inline bool full() const { return size_ == Configs::CHUNK_SIZE; }
  vid_t operator[](size_t i) const { return data_[i]; }
  vid_t& operator[](size_t i) { return data_[i]; }

  bool has_value(size_t idx) const {
    return data_[idx] != std::numeric_limits<vid_t>::max();
  }

  VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kSingle;
  }

  std::unordered_set<label_t> get_labels_set() const override {
    return {label_};
  }

  bool is_optional() const override { return is_optional_; }

  std::string column_info() const override {
    return "SLVertexColumn(" + to_string(static_cast<int>(label_)) + ")[" +
           std::to_string(size_) + "]";
  }
  VertexRecord get_vertex(size_t idx) const override {
    return VertexRecord{label_, data_[idx]};
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      func(i, label_, data_[i]);
    }
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func,
                      const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      size_t len = offsets[i] & 0xFFFFFFFF;
      size_t offset = offsets[i] >> 32;
      for (size_t j = 0; j < len; ++j) {
        size_t idx = offset + j;
        size_t index = i << 32 | idx;
        func(index, label_, data_[idx]);
      }
    }
  }

  std::shared_ptr<IContextColumn> project(
      const gs::runtime::GraphReadInterface&, const std::string& property_name,
      const RTAnyType& type) override;
  bool is_optional_;
  label_t label_;
  size_t size_;
  vid_t* data_;
  const LocalMemPool& local_pool_;
};

class MLVertexColumn : public IVertexColumn {
 public:
  MLVertexColumn(const LocalMemPool& mem_pool)
      : is_optional_(false),
        size_(0),
        data_(static_cast<VertexRecord*>(
            mem_pool.Allocate(sizeof(VertexRecord) * Configs::CHUNK_SIZE))),
        local_pool_(mem_pool) {}
  MLVertexColumn(const LocalMemPool& mem_pool,
                 const std::unordered_set<label_t>& labels)
      : is_optional_(false),
        size_(0),
        data_(static_cast<VertexRecord*>(
            mem_pool.Allocate(sizeof(VertexRecord) * Configs::CHUNK_SIZE))),
        labels_(labels),
        local_pool_(mem_pool) {}

  ~MLVertexColumn() {
    if (data_) {
      local_pool_.Deallocate(data_, sizeof(VertexRecord) * Configs::CHUNK_SIZE);
      data_ = nullptr;
    }
  }
  size_t size() const override { return size_; }

  void clear() override { size_ = 0; }
  void emplace_back(VertexRecord&& value) {
    labels_.insert(value.label_);
    data_[size_++] = std::move(value);
  }
  void push_back(const VertexRecord& value) {
    labels_.insert(value.label_);
    data_[size_++] = value;
  }

  void push_back_opt(label_t label, vid_t vid) {
    labels_.insert(label);
    data_[size_++] = VertexRecord{label, vid};
  }

  void push_back_null() {
    is_optional_ = true;
    data_[size_] = VertexRecord{std::numeric_limits<label_t>::max(),
                                std::numeric_limits<vid_t>::max()};
  }
  bool full() const { return size_ == Configs::CHUNK_SIZE; }
  VertexRecord operator[](size_t i) const { return data_[i]; }
  VertexRecord& operator[](size_t i) { return data_[i]; }
  VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kMultiple;
  }

  std::unordered_set<label_t> get_labels_set() const override {
    return labels_;
  }

  bool is_optional() const override { return is_optional_; }

  bool has_value(size_t idx) const {
    return data_[idx].vid_ != std::numeric_limits<vid_t>::max();
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      func(i, data_[i].label_, data_[i].vid_);
    }
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func,
                      const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      size_t len = offsets[i] & 0xFFFFFFFF;
      size_t offset = offsets[i] >> 32;
      for (size_t j = 0; j < len; ++j) {
        size_t idx = offset + j;
        size_t index = i << 32 | idx;
        func(index, data_[idx].label_, data_[idx].vid_);
      }
    }
  }
  VertexRecord get_vertex(size_t idx) const override {
    return data_[idx & 0xFFFFFFFF];
  }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  std::string column_info() const override {
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "MLVertexColumn(" + labels + ")[" + std::to_string(size()) + "]";
  }

  std::shared_ptr<IContextColumn> project(
      const gs::runtime::GraphReadInterface&, const std::string& property_name,
      const RTAnyType& type) override;

  bool is_optional_ = false;
  size_t size_;
  VertexRecord* data_;
  std::unordered_set<label_t> labels_;
  const LocalMemPool& local_pool_;
};

template <typename FUNC_T>
inline void IVertexColumn::foreach_vertex(const FUNC_T& func) const {
  if (vertex_column_type() == VertexColumnType::kSingle) {
    static_cast<const SLVertexColumn*>(this)->foreach_vertex(func);
  } else {
    static_cast<const MLVertexColumn*>(this)->foreach_vertex(func);
  }
}

template <typename FUNC_T>
inline void IVertexColumn::foreach_vertex(
    const FUNC_T& func, const ValueColumn<size_t>& offsets) const {
  if (vertex_column_type() == VertexColumnType::kSingle) {
    static_cast<const SLVertexColumn*>(this)->foreach_vertex(func, offsets);
  } else {
    static_cast<const MLVertexColumn*>(this)->foreach_vertex(func, offsets);
  }
}
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VERTEX_COLUMNS_H_