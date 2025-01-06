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
#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/project.h"

#include <queue>

namespace gs {

namespace runtime {

template <typename T>
class AscValue {
 public:
  AscValue() : val_() {}
  AscValue(const T& val) : val_(val) {}

  bool operator<(const AscValue& rhs) const { return val_ < rhs.val_; }

  const T& value() const { return val_; }

 private:
  T val_;
};

template <typename VAR_T>
class AscWrapper {
 public:
  using elem_t = typename VAR_T::elem_t;
  using value_t = AscValue<elem_t>;

  AscWrapper(VAR_T&& var) : var_(std::move(var)) {}
  AscValue<elem_t> get(size_t idx) const {
    return AscValue<elem_t>(var_.typed_eval_path(idx));
  }

 private:
  VAR_T var_;
};

template <typename T>
class DescValue {
 public:
  DescValue() : val_() {}
  DescValue(const T& val) : val_(val) {}

  bool operator<(const DescValue& rhs) const { return rhs.val_ < val_; }

  const T& value() const { return val_; }

 private:
  T val_;
};

template <typename VAR_T>
class DescWrapper {
 public:
  using elem_t = typename VAR_T::elem_t;
  using value_t = DescValue<elem_t>;

  DescWrapper(VAR_T&& var) : var_(std::move(var)) {}
  DescValue<elem_t> get(size_t idx) const {
    return DescValue<elem_t>(var_.typed_eval_path(idx));
  }

 private:
  VAR_T var_;
};

class OrderBy {
 public:
  template <typename Comparer>
  static Context order_by_with_limit(const GraphReadInterface& graph,
                                     Context&& ctx, const Comparer& cmp,
                                     size_t low, size_t high) {
    if (low == 0 && high >= ctx.row_num()) {
      std::vector<size_t> offsets(ctx.row_num());
      std::iota(offsets.begin(), offsets.end(), 0);
      std::sort(offsets.begin(), offsets.end(),
                [&](size_t lhs, size_t rhs) { return cmp(lhs, rhs); });
      ctx.reshuffle(offsets);
      return ctx;
    }
    size_t row_num = ctx.row_num();
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (size_t i = 0; i < row_num; ++i) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
    return ctx;
  }

  template <typename Comparer>
  static Context staged_order_by_with_limit(
      const GraphReadInterface& graph, Context&& ctx, const Comparer& cmp,
      size_t low, size_t high, const std::vector<size_t>& indices) {
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (auto i : indices) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
    return ctx;
  }
};
}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_