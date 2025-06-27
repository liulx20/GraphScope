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

#ifndef RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_

#include <memory>
#include <string>

#include "flex/engines/graph_db/runtime/common/rt_any.h"

#include "glog/logging.h"

namespace gs {

namespace runtime {

enum class ContextColumnType {
  kVertex,
  kEdge,
  kValue,
  kPath,
  kOptionalValue,
};

class IContextColumnBuilder;
class IOptionalContextColumnBuilder;

class IContextColumn {
 public:
  IContextColumn() = default;
  virtual ~IContextColumn() = default;

  virtual size_t size() const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return 0;
  }

  virtual std::string column_info() const = 0;
  virtual ContextColumnType column_type() const = 0;

  virtual RTAnyType elem_type() const = 0;

  virtual std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual RTAny get_elem(size_t idx) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return RTAny();
  }

  virtual bool has_value(size_t idx) const { return true; }

  virtual bool is_optional() const { return false; }

  virtual void generate_dedup_offset(std::vector<size_t>& offsets) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  virtual bool order_by_limit(bool asc, size_t limit,
                              std::vector<size_t>& offsets) const {
    LOG(INFO) << "order by limit not implemented for " << this->column_info();
    return false;
  }

  virtual std::shared_ptr<Arena> get_arena() const { return nullptr; }

  virtual void set_arena(const std::shared_ptr<Arena>& arena) {}
};

class IContextColumnBuilder {
 public:
  IContextColumnBuilder() = default;
  virtual ~IContextColumnBuilder() = default;

  virtual void reserve(size_t size) = 0;
  virtual void push_back_elem(const RTAny& val) = 0;

  virtual std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& ptr) = 0;
};

class IOptionalContextColumnBuilder : public IContextColumnBuilder {
 public:
  IOptionalContextColumnBuilder() = default;
  virtual ~IOptionalContextColumnBuilder() = default;

  virtual void push_back_null() = 0;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_