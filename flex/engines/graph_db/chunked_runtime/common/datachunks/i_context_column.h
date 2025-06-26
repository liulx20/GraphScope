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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_I_CONTEXT_COLUMN_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_I_CONTEXT_COLUMN_H_

#include <memory>

#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {
namespace chunked_runtime {
using gs::runtime::Arena;
using gs::runtime::RTAny;
using gs::runtime::RTAnyType;
enum class ContextColumnType { kVertex, kEdge, kValue, kPath };

class IContextColumn {
 public:
  virtual ~IContextColumn() = default;

  // Get the type of the column
  virtual ContextColumnType column_type() const = 0;

  // Get the number of elements in the column
  virtual size_t size() const = 0;

  virtual void clear() = 0;

  // Get the element at the specified index
  virtual RTAny get_elem(size_t idx) const = 0;

  virtual bool is_optional() const = 0;

  virtual std::string column_info() const = 0;

  virtual RTAnyType elem_type() const = 0;

  virtual std::shared_ptr<Arena> get_arena() const { return nullptr; }

  virtual void set_arena(const std::shared_ptr<Arena>&) {}

  virtual bool has_value(size_t idx) const = 0;
};

}  // namespace chunked_runtime
}  // namespace gs
#endif