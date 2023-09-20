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

#ifndef GRAPHSCOPE_PROPERTY_COLUMN_H_
#define GRAPHSCOPE_PROPERTY_COLUMN_H_

#include <string>
#include <string_view>

#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& filename) = 0;

  virtual void resize(size_t size) = 0;

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;

  virtual Any get(size_t index) const = 0;

  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual StorageStrategy storage_strategy() const = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void open(const std::string& filename) override { buffer_.open(filename); }

  void resize(size_t size) override { buffer_.resize(size); }

  PropertyType type() const override { return AnyConverter<T>::type; }

  void set_value(size_t index, const T& val) { buffer_.set(index, val); }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  T get_view(size_t index) const { return buffer_.get(index); }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(buffer_.get(index));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<T>& buffer() const { return buffer_; }

 private:
  mmap_array<T> buffer_;
  StorageStrategy strategy_;
};

using IntColumn = TypedColumn<int>;
using LongColumn = TypedColumn<int64_t>;
using DateColumn = TypedColumn<Date>;

class StringColumn : public ColumnBase {
 public:
  StringColumn(StorageStrategy strategy, size_t width = 1024) : width_(width) {}
  ~StringColumn() {}

  void open(const std::string& filename) override {
    buffer_.open(filename);
    pos_.store(buffer_.data_size());
  }

  void resize(size_t size) override {
    if (size > buffer_.size()) {
      buffer_.resize(size, width_ * size);
    } else {
      buffer_.resize(size, pos_.load());
    }
  }

  PropertyType type() const override {
    return AnyConverter<std::string_view>::type;
  }

  void set_value(size_t idx, const std::string_view& val) {
    size_t offset = pos_.fetch_add(val.size());
    buffer_.set(idx, offset, val);
  }

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, AnyConverter<std::string_view>::from_any(value));
  }

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(buffer_.get(idx));
  }

  std::string_view get_view(size_t idx) const { return buffer_.get(idx); }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<std::string_view>& buffer() const { return buffer_; }

 private:
  mmap_array<std::string_view> buffer_;
  std::atomic<size_t> pos_;
  StorageStrategy strategy_;
  size_t width_;
};

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer, StorageStrategy strategy)
      : buffer_(buffer), strategy_(strategy) {}
  TypedRefColumn(const TypedColumn<T>& column)
      : buffer_(column.buffer()), strategy_(column.storage_strategy()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const { return buffer_.get(index); }

 private:
  const mmap_array<T>& buffer_;
  StorageStrategy strategy_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
