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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VALUE_COLUMNS_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VALUE_COLUMNS_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/i_context_column.h"
#include "flex/engines/graph_db/chunked_runtime/utils/configs.h"

namespace gs {
namespace chunked_runtime {
using gs::runtime::List;
template <typename T>
class ValueColumn : public IContextColumn {
 public:
  ValueColumn()
      : size_(0),
        data_(std::make_unique<T[]>(Configs::CHUNK_SIZE)),
        valid_(nullptr),
        arena_(nullptr) {}

  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  ValueColumn(ValueColumn<T>&& other)
      : is_optional_(other.is_optional_),
        size_(other.size_),
        data_(std::move(other.data_)),
        valid_(std::move(other.valid_)),
        arena_(std::move(other.arena_)) {
    other.size_ = 0;
    other.data_ = nullptr;
    other.valid_ = nullptr;
  }

  ValueColumn(const ValueColumn<T>& other)
      : is_optional_(other.is_optional_),
        size_(other.size_),
        data_(std::make_unique<T[]>(Configs::CHUNK_SIZE)),
        valid_(other.valid_
                   ? std::make_unique<uint8_t[]>(Configs::CHUNK_SIZE / 8)
                   : nullptr),
        arena_(other.arena_) {
    std::copy(other.data_.get(), other.data_.get() + size_, data_.get());
    if (valid_) {
      std::copy(other.valid_.get(),
                other.valid_.get() + Configs::CHUNK_SIZE / 8, valid_.get());
    }
  }

  void clear() override { size_ = 0; }
  size_t size() const override { return size_; }

  inline void emplace_back(T&& value) { data_[size_++] = std::move(value); }

  inline void push_back(const T& value) { data_[size_++] = value; }

  inline void push_back_null() {
    is_optional_ = true;
    if (valid_ == nullptr) {
      valid_ = std::make_unique<uint8_t[]>(Configs::CHUNK_SIZE / 8);
      memset(valid_.get(), 0, Configs::CHUNK_SIZE / 8 * sizeof(uint8_t));
    }
    valid_[size_ / 8] |= (1 << (size_ % 8));
    data_[size_++] =
        T();  // Assuming T has a default constructor for null value
  }

  bool has_value(size_t idx) const override {
    if (valid_ == nullptr) {
      return true;  // No validity mask, all values are considered present
    }
    return (valid_[idx / 8] & (1 << (idx % 8))) != 0;
  }

  inline bool full() const { return size_ == Configs::CHUNK_SIZE; }
  T operator[](size_t i) const { return data_[i]; }
  T& operator[](size_t i) { return data_[i]; }

  RTAny get_elem(size_t idx) const override {
    return gs::runtime::TypedConverter<T>::from_typed(data_[idx]);
  }

  bool is_optional() const override { return is_optional_; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }
  std::shared_ptr<Arena> get_arena() const override { return arena_; }
  RTAnyType elem_type() const override {
    return gs::runtime::TypedConverter<T>::type();
  }
  std::string column_info() const override {
    return "ValueColumn[" + std::to_string(size_) + "]";
  }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override {
    auto ptr = std::make_shared<ValueColumn<T>>();

    if (!shift) {
      for (size_t i = 0; i < size_; ++i) {
        ptr->data_[i] = data_[offsets[i] & 0xFFFFFFFF];
      }
    } else {
      for (size_t i = 0; i < size_; ++i) {
        ptr->data_[i] = data_[offsets[i] >> 32];
      }
    }

    if (is_optional_) {
      ptr->is_optional_ = true;
      ptr->valid_ = std::make_unique<uint8_t[]>(Configs::CHUNK_SIZE / 8);
      for (size_t i = 0; i < size_; ++i) {
        int offset = shift ? offsets[i] >> 32 : offsets[i] & 0xFFFFFFFF;
        uint8_t offset_byte = (valid_[offset / 8]) >> (offset % 8);
        ptr->valid_[i / 8] |= (offset_byte << (i % 8));
      }
    }

    ptr->size_ = size_;
    return ptr;
  }

 private:
  bool is_optional_;
  size_t size_;
  std::unique_ptr<T[]> data_;
  std::unique_ptr<uint8_t[]> valid_;
  std::shared_ptr<Arena> arena_;
};

template <>
class ValueColumn<List> : public IContextColumn {
 public:
  ValueColumn(RTAnyType type)
      : elem_type_(type),
        size_(0),
        data_(std::make_unique<List[]>(Configs::CHUNK_SIZE)) {}
  ~ValueColumn() = default;

  size_t size() const override { return size_; }

  std::string column_info() const override {
    return "ListValueColumn[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  RTAnyType elem_type() const override {
    auto type = RTAnyType::kList;
    return type;
  }
  RTAny get_elem(size_t idx) const override {
    return RTAny::from_list(data_[idx]);
  }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

  bool is_optional() const override { return false; }

  bool has_value(size_t idx) const override {
    // In this case, we assume all elements are valid since List does not have
    // a validity mask.
    return true;
  }

  void clear() override { size_ = 0; }

  inline void emplace_back(List&& value) { data_[size_++] = std::move(value); }

  inline void push_back(const List& value) { data_[size_++] = value; }

  inline void push_back_null() {
    // Assuming List has a default constructor for null value
    LOG(FATAL) << "Cannot push back null for List in ValueColumn";
    data_[size_++] = List();
  }
  inline bool full() const { return size_ == Configs::CHUNK_SIZE; }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

 private:
  RTAnyType elem_type_;
  size_t size_;
  std::unique_ptr<List[]> data_;

  std::shared_ptr<Arena> arena_;
};

}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_VALUE_COLUMNS_H_