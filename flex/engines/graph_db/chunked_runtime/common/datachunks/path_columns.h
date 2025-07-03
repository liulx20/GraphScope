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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_PATH_COLUMNS_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_PATH_COLUMNS_H_
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/i_context_column.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/utils/configs.h"
namespace gs {
namespace chunked_runtime {
using gs::runtime::Path;
using gs::runtime::PathImpl;
class IPathColumn : public IContextColumn {
 public:
  IPathColumn() = default;
  virtual ~IPathColumn() = default;

  virtual const Path& get_path(size_t idx) const = 0;
  virtual int get_path_length(size_t idx) const {
    return get_path(idx).len() - 1;
  }

  template <typename FUNC_T>
  void foreach_path(const FUNC_T& func) const;

  template <typename FUNC_T>
  void foreach_path(const FUNC_T& func,
                    const ValueColumn<size_t>& offsets) const;
};

class GeneralPathColumn : public IPathColumn {
 public:
  GeneralPathColumn() {
    size_ = 0;
    data_ = std::make_unique<Path[]>(Configs::CHUNK_SIZE);
    valid_ = nullptr;
    is_optional_ = false;
  }
  GeneralPathColumn(GeneralPathColumn&& other)
      : is_optional_(other.is_optional_),
        size_(other.size_),
        data_(std::move(other.data_)),
        valid_(std::move(other.valid_)),
        arena_(std::move(other.arena_)) {}
  ~GeneralPathColumn() {}
  inline size_t size() const override { return size_; }
  std::string column_info() const override {
    return "GeneralPathColumn[" + std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kPath;
  }

  inline RTAnyType elem_type() const override { return RTAnyType::kPath; }
  inline RTAny get_elem(size_t idx) const override { return RTAny(data_[idx]); }
  inline const Path& get_path(size_t idx) const override { return data_[idx]; }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }
  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

  bool is_optional() const override { return is_optional_; }

  void push_back(const Path& path) { data_[size_++] = path; }

  inline void push_back_opt(std::unique_ptr<PathImpl>&& impl) {
    data_[size_++] = Path(impl.get());
    arena_->emplace_back(std::move(impl));
  }

  void emplace_back(Path&& path) { data_[size_++] = std::move(path); }

  void push_back_null() {
    is_optional_ = true;
    if (valid_ == nullptr) {
      valid_ = std::make_unique<uint8_t[]>(Configs::CHUNK_SIZE / 8);
      memset(valid_.get(), 0, Configs::CHUNK_SIZE / 8 * sizeof(uint8_t));
    }
    valid_[size_ / 8] |= (1 << (size_ % 8));
    data_[size_++] =
        Path();  // Assuming Path has a default constructor for null value
  }

  bool has_value(size_t idx) const override {
    if (valid_ == nullptr) {
      return true;  // No validity mask, all values are considered present
    }
    return (valid_[idx / 8] & (1 << (idx % 8))) != 0;
  }

  bool full() const { return size_ == Configs::CHUNK_SIZE; }

  void clear() override {
    size_ = 0;
    is_optional_ = false;
    if (valid_ != nullptr) {
      memset(valid_.get(), 0, Configs::CHUNK_SIZE / 8 * sizeof(uint8_t));
    }
  }

  Path operator[](size_t i) const { return data_[i]; }
  Path& operator[](size_t i) { return data_[i]; }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  template <typename FUNC_T>
  void foreach_path(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      func(i, data_[i]);
    }
  }

  template <typename FUNC_T>
  void foreach_path(const FUNC_T& func,
                    const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      int len = offsets[i] & 0xFFFFFFFF;
      int offset = offsets[i] >> 32;
      for (int j = 0; j < len; ++j) {
        int idx = offset + j;
        size_t index = (i << 32) | idx;
        func(index, data_[idx]);
      }
    }
  }

 private:
  bool is_optional_;
  size_t size_;
  std::unique_ptr<Path[]> data_;
  std::unique_ptr<uint8_t[]> valid_;
  std::shared_ptr<Arena> arena_;
};

template <typename FUNC_T>
inline void IPathColumn::foreach_path(const FUNC_T& func) const {
  static_cast<const GeneralPathColumn*>(this)->foreach_path(func);
}
template <typename FUNC_T>
inline void IPathColumn::foreach_path(
    const FUNC_T& func, const ValueColumn<size_t>& offsets) const {
  static_cast<const GeneralPathColumn*>(this)->foreach_path(func, offsets);
}
}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_PATH_COLUMNS_H_