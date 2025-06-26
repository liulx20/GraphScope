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

#ifndef CHUNKED_RUNTIME_UTILS_EXPRS_WITH_IN_EXPR_H_
#define CHUNKED_RUNTIME_UTILS_EXPRS_WITH_IN_EXPR_H_
#include "flex/engines/graph_db/chunked_runtime/utils/exprs/expr_base.h"

namespace gs {
namespace chunked_runtime {
class VertexWithInSetExpr : public ExprBase {
 public:
  VertexWithInSetExpr(std::unique_ptr<ExprBase>&& key,
                      std::unique_ptr<ExprBase>&& val_set)
      : key_(std::move(key)), val_set_(std::move(val_set)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_set_->type() == RTAnyType::kSet);
  }
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto key = key_->eval_path(idx, arena).as_vertex();
    auto set = val_set_->eval_path(idx, arena).as_set();
    assert(set.impl_ != nullptr);
    auto ptr = dynamic_cast<gs::runtime::SetImpl<VertexRecord>*>(set.impl_);
    assert(ptr != nullptr);
    return RTAny::from_bool(ptr->exists(key));
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto key = key_->eval_vertex(label, v, arena).as_vertex();
    auto set = val_set_->eval_vertex(label, v, arena).as_set();
    return RTAny::from_bool(
        dynamic_cast<gs::runtime::SetImpl<VertexRecord>*>(set.impl_)->exists(
            key));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    auto key = key_->eval_edge(label, src, dst, data, arena).as_vertex();
    auto set = val_set_->eval_edge(label, src, dst, data, arena).as_set();
    return RTAny::from_bool(
        dynamic_cast<gs::runtime::SetImpl<VertexRecord>*>(set.impl_)->exists(
            key));
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_set_;
};
class VertexWithInListExpr : public ExprBase {
 public:
  VertexWithInListExpr(std::unique_ptr<ExprBase>&& key,
                       std::unique_ptr<ExprBase>&& val_list)
      : key_(std::move(key)), val_list_(std::move(val_list)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_list_->type() == RTAnyType::kList);
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto key = key_->eval_path(idx, arena).as_vertex();
    auto list = val_list_->eval_path(idx, arena).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    auto key = key_->eval_vertex(label, v, arena).as_vertex();
    auto list = val_list_->eval_vertex(label, v, arena).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    auto key = key_->eval_edge(label, src, dst, data, arena).as_vertex();
    auto list = val_list_->eval_edge(label, src, dst, data, arena).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_list_;
};

#define PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(dst_vector_name, array_name) \
  size_t len = array_name.item_size();                                   \
  for (size_t idx = 0; idx < len; ++idx) {                               \
    dst_vector_name.push_back(array_name.item(idx));                     \
  }

template <typename T>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(std::unique_ptr<ExprBase>&& key, const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        // TODO(zhanglei,lexiao): We should support more types here, and if type
        // conversion fails, we should return an error.
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else if constexpr (std::is_same_v<T, int64_t>) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int32_t array";
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(array.item_case() == common::Value::kStrArray);
      PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.str_array());
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_path(idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val =
          gs::runtime::TypedConverter<T>::to_typed(key_->eval_path(idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto any_val = key_->eval_path(idx, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }
  RTAny eval_vertex(label_t label, vid_t v, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_vertex(label, v, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = gs::runtime::TypedConverter<T>::to_typed(
          key_->eval_vertex(label, v, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_vertex(label_t label, vid_t v, Arena& arena, int) const override {
    auto any_val = key_->eval_vertex(label, v, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, arena);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(
          key_->eval_edge(label, src, dst, data, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = gs::runtime::TypedConverter<T>::to_typed(
          key_->eval_edge(label, src, dst, data, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
    return RTAny::from_bool(false);
  }
  RTAnyType type() const override { return RTAnyType::kBoolValue; }
  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;

  std::vector<T> container_;
};

}  // namespace chunked_runtime
}  // namespace gs

#endif  // CHUNKED_RUNTIME_UTILS_EXPRS_WITH_IN_EXPR_H_