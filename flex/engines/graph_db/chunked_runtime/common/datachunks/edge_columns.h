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

#ifndef CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_EDGE_COLUMNS_H_
#define CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_EDGE_COLUMNS_H_
#include <variant>
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/i_context_column.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/utils/configs.h"
namespace gs {
namespace chunked_runtime {
enum class EdgeColumnType { kSDSL, kSDML, kBDSL, kBDML };
using gs::runtime::Direction;
using gs::runtime::EdgeData;
using gs::runtime::EdgePropVec;
using gs::runtime::EdgeRecord;
using gs::runtime::LabelTriplet;
using gs::runtime::RTAnyType;
struct edge_property_vec {
  std::variant<EdgePropVec<grape::EmptyType>, EdgePropVec<int64_t>,
               EdgePropVec<int32_t>, EdgePropVec<double>,
               EdgePropVec<std::string_view>, EdgePropVec<bool>,
               EdgePropVec<Date>, EdgePropVec<Day>, EdgePropVec<RecordView>>
      edge_col_;
  edge_property_vec() = default;
  template <typename T>
  edge_property_vec(const EdgePropVec<T>& col) : edge_col_(col) {}

  edge_property_vec(edge_property_vec&& other) noexcept
      : edge_col_(std::move(other.edge_col_)) {}
  edge_property_vec(const edge_property_vec& other)
      : edge_col_(other.edge_col_) {}
  edge_property_vec& operator=(edge_property_vec&& other) noexcept {
    edge_col_ = std::move(other.edge_col_);
    return *this;
  }
  inline static edge_property_vec create_edge_prop_vec(PropertyType type) {
    if (type == PropertyType::Int64()) {
      return EdgePropVec<int64_t>();
    } else if (type == PropertyType::StringView()) {
      return EdgePropVec<std::string_view>();
    } else if (type == PropertyType::Date()) {
      return EdgePropVec<Date>();
    } else if (type == PropertyType::Day()) {
      return EdgePropVec<Day>();
    } else if (type == PropertyType::Int32()) {
      return EdgePropVec<int32_t>();
    } else if (type == PropertyType::Double()) {
      return EdgePropVec<double>();
    } else if (type == PropertyType::Bool()) {
      return EdgePropVec<bool>();
    } else if (type == PropertyType::Empty()) {
      return EdgePropVec<grape::EmptyType>();
    } else if (type == PropertyType::RecordView()) {
      return EdgePropVec<RecordView>();
    } else {
      LOG(FATAL) << "not support for " << type;
      return EdgePropVec<grape::EmptyType>();
    }
  }

  inline EdgeData get(size_t idx) const {
    return std::visit(
        [idx](const auto& col) { return EdgeData(col.get_view(idx)); },
        edge_col_);
  }

  inline void get_edge_data(EdgeData& edge_record, size_t idx) const {
    std::visit([&edge_record,
                idx](auto&& col) { edge_record = EdgeData(col.get_view(idx)); },
               edge_col_);
  }

  inline void set_edge_data(const EdgeData& edge_record, size_t idx) {
    std::visit(
        [idx, &edge_record](auto& col) {
          using T = typename std::decay_t<decltype(col)>::EdgeDataType;
          col.set(idx, edge_record.as<T>());
        },
        edge_col_);
  }

  inline void set_null(size_t idx) {
    std::visit(
        [idx](auto& col) {
          using T = typename std::decay_t<decltype(col)>::EdgeDataType;
          col.set(idx, T());
        },
        edge_col_);
  }

  inline size_t size() const {
    return std::visit([](const auto& col) { return col.size(); }, edge_col_);
  }

  inline void resize(size_t size) {
    std::visit([size](auto& col) { col.resize(size); }, edge_col_);
  }

  inline void set_any(size_t idx, const edge_property_vec& other,
                      size_t other_idx) {
    std::visit(
        [idx, other, other_idx](auto& col) {
          auto data = other.get(other_idx);
          col.set(
              idx,
              data.as<typename std::decay_t<decltype(col)>::EdgeDataType>());
        },
        edge_col_);
  }

  void clear() {
    std::visit([](auto& col) { col.clear(); }, edge_col_);
  }
};

struct LabelTripletHash {
  std::size_t operator()(const LabelTriplet& lt) const {
    size_t val = static_cast<size_t>(lt.src_label) << 32 |
                 static_cast<size_t>(lt.dst_label) << 16 |
                 static_cast<size_t>(lt.edge_label);
    return std::hash<size_t>()(val);
  }
};

class IEdgeColumn : public IContextColumn {
 public:
  IEdgeColumn() = default;
  virtual ~IEdgeColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kEdge;
  }

  virtual EdgeRecord get_edge(size_t idx) const = 0;

  inline RTAny get_elem(size_t idx) const override {
    return RTAny::from_edge(this->get_edge(idx));
  }

  inline RTAnyType elem_type() const override { return RTAnyType::kEdge; }

  virtual Direction dir() const = 0;
  virtual std::vector<LabelTriplet> get_labels() const = 0;
  virtual EdgeColumnType edge_column_type() const = 0;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func,
                    const ValueColumn<size_t>& offsets) const;
};

class SDSLEdgeColumn : public IEdgeColumn {
 public:
  SDSLEdgeColumn(Direction dir, const LabelTriplet& label,
                 PropertyType prop_type)
      : dir_(dir),
        label_(label),
        size_(0),
        prop_type_(prop_type),
        prop_col_(edge_property_vec::create_edge_prop_vec(prop_type)) {
    edges_ = std::make_unique<std::pair<vid_t, vid_t>[]>(
        gs::chunked_runtime::Configs::CHUNK_SIZE);
    is_optional_ = false;
  }

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    ret.label_triplet_ = label_;
    ret.src_ = edges_[idx].first;
    ret.dst_ = edges_[idx].second;
    prop_col_.get_edge_data(ret.prop_, idx);
    ret.dir_ = dir_;
    return ret;
  }

  inline size_t size() const override { return size_; }

  inline Direction dir() const override { return dir_; }

  std::string column_info() const override {
    return "SDSLEdgeColumn: label = " + label_.to_string() +
           ", dir = " + std::to_string((int) dir_) +
           ", size = " + std::to_string(size_);
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

  inline void push_back_elem(const RTAny& val) {
    const auto& e = val.as_edge();
    push_back_opt(e.src_, e.dst_, e.prop_);
  }
  inline void push_back_opt(vid_t src, vid_t dst, const EdgeData& data) {
    prop_col_.set_edge_data(data, size_);
    edges_[size_++] = std::make_pair(src, dst);
  }

  inline void push_back_null() {
    is_optional_ = true;
    edges_[size_] = std::make_pair(std::numeric_limits<vid_t>::max(),
                                   std::numeric_limits<vid_t>::max());
    prop_col_.set_null(size_++);
  }

  inline bool full() const {
    return size_ == gs::chunked_runtime::Configs::CHUNK_SIZE;
  }
  inline bool is_optional() const { return is_optional_; }
  inline bool has_value(size_t idx) const {
    return edges_[idx].first != std::numeric_limits<vid_t>::max() &&
           edges_[idx].second != std::numeric_limits<vid_t>::max();
  }

  inline void clear() override {
    prop_col_.clear();
    size_ = 0;
    is_optional_ = false;
  }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      func(i, label_, edges_[i].first, edges_[i].second, prop_col_.get(i),
           dir_);
    }
  }

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func,
                           const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      int len = offsets[i] & 0xFFFFFFFF;
      int offset = offsets[i] >> 32;
      for (int j = 0; j < len; ++j) {
        size_t idx = offset + j;
        size_t index = (i << 32) | idx;
        func(index, label_, edges_[idx].first, edges_[idx].second,
             prop_col_.get(idx), dir_);
      }
    }
  }

  PropertyType prop_type() const { return prop_type_; }

  bool is_optional_ = false;
  Direction dir_;
  LabelTriplet label_;
  size_t size_;
  std::unique_ptr<std::pair<vid_t, vid_t>[]> edges_;
  PropertyType prop_type_;
  edge_property_vec prop_col_;
};

class SDMLEdgeColumn : public IEdgeColumn {
 public:
  SDMLEdgeColumn(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : dir_(dir), size_(0) {
    edges_ = std::make_unique<std::tuple<label_t, vid_t, vid_t, int>[]>(
        gs::chunked_runtime::Configs::CHUNK_SIZE);
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      edge_labels_.emplace_back(label);
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          edge_property_vec::create_edge_prop_vec(label.second);
    }
    is_optional_ = false;
  }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  inline EdgeRecord get_edge(size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = edge_labels_[index].first;
    auto offset = std::get<3>(e);
    EdgeRecord ret;
    ret.label_triplet_ = label;
    ret.src_ = std::get<1>(e);
    ret.dst_ = std::get<2>(e);
    prop_cols_[index].get_edge_data(ret.prop_, offset);

    ret.dir_ = dir_;
    return ret;
  }

  inline size_t size() const override { return size_; }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < edge_labels_.size(); ++idx) {
      auto label = edge_labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "SDMLEdgeColumn: label = {" + ss.str() +
           "}, dir = " + std::to_string((int) dir_) +
           ", size = " + std::to_string(size_);
  }

  std::vector<LabelTriplet> get_labels() const override {
    std::vector<LabelTriplet> labels;
    for (auto& label : edge_labels_) {
      labels.push_back(label.first);
    }
    return labels;
  }

  inline Direction dir() const { return dir_; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDML;
  }

  inline void push_back_opt(LabelTriplet label, vid_t src, vid_t dst,
                            const EdgeData& data) {
    auto index = index_[label];
    size_t len = prop_cols_[index].size();
    prop_cols_[index].set_edge_data(data, len);
    edges_[size_++] = std::make_tuple(index, src, dst, len);
  }

  inline void push_back_elem(const RTAny& val) {
    const auto& e = val.as_edge();
    push_back_opt(e.label_triplet_, e.src_, e.dst_, e.prop_);
  }

  inline void push_back_null() {
    size_t len = prop_cols_[0].size();

    is_optional_ = true;
    prop_cols_[0].set_null(len);
    edges_[size_++] = std::make_tuple(0, std::numeric_limits<vid_t>::max(),
                                      std::numeric_limits<vid_t>::max(), len);
  }

  inline bool full() const {
    return size_ == gs::chunked_runtime::Configs::CHUNK_SIZE;
  }

  inline bool is_optional() const { return is_optional_; }

  inline bool has_value(size_t idx) const {
    auto& e = edges_[idx];
    return std::get<1>(e) != std::numeric_limits<vid_t>::max() &&
           std::get<2>(e) != std::numeric_limits<vid_t>::max();
  }

  inline void clear() override {
    size_ = 0;
    is_optional_ = false;
    for (auto& col : prop_cols_) {
      col.clear();
    }
  }

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      auto& e = edges_[i];
      auto index = std::get<0>(e);
      auto label = edge_labels_[index].first;
      auto src = std::get<1>(e);
      auto dst = std::get<2>(e);
      auto offset = std::get<3>(e);
      func(i, label, src, dst, prop_cols_[index].get(offset),
           dir_);  // dir_ is always the same for SDMLEdgeColumn
    }
  }
  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func,
                           const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      int len = offsets[i] & 0xFFFFFFFF;
      int offset = offsets[i] >> 32;
      for (int j = 0; j < len; ++j) {
        size_t idx = offset + j;
        const auto& e = edges_[idx];
        auto index = std::get<0>(e);
        auto label = edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto offset = std::get<3>(e);
        size_t index2 = (i << 32) | idx;
        func(index2, label, src, dst, prop_cols_[index].get(offset), dir_);
      }
    }
  }

  bool is_optional_ = false;
  Direction dir_;
  std::unordered_map<LabelTriplet, label_t, LabelTripletHash> index_;
  size_t size_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::unique_ptr<std::tuple<label_t, vid_t, vid_t, int>[]> edges_;
  std::vector<edge_property_vec> prop_cols_;
};

class BDSLEdgeColumn : public IEdgeColumn {
 public:
  BDSLEdgeColumn(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(edge_property_vec::create_edge_prop_vec(prop_type)) {
    edges_ = std::make_unique<std::tuple<vid_t, vid_t, bool>[]>(
        gs::chunked_runtime::Configs::CHUNK_SIZE);
    size_ = 0;
    is_optional_ = false;
  }

  inline void push_back_opt(vid_t src, vid_t dst, const EdgeData& data,
                            Direction dir) {
    edges_[size_] = std::make_tuple(src, dst, dir == Direction::kOut);
    prop_col_.set_edge_data(data, size_);
    size_++;
  }

  inline void push_back_null() {
    is_optional_ = true;
    edges_[size_] = std::make_tuple(std::numeric_limits<vid_t>::max(),
                                    std::numeric_limits<vid_t>::max(), false);
    prop_col_.set_null(size_);
    size_++;
  }

  inline EdgeRecord get_edge(size_t idx) const override {
    auto src = std::get<0>(edges_[idx]);
    auto dst = std::get<1>(edges_[idx]);
    auto dir = std::get<2>(edges_[idx]);
    EdgeRecord ret;
    ret.label_triplet_ = label_;
    ret.src_ = src;
    ret.dst_ = dst;
    prop_col_.get_edge_data(ret.prop_, idx);
    ret.dir_ = (dir ? Direction::kOut : Direction::kIn);
    return ret;
  }

  inline size_t size() const override { return size_; }

  std::string column_info() const override {
    return "BDSLEdgeColumn: label = " + label_.to_string() +
           ", size = " + std::to_string(size_);
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

  inline RTAnyType elem_type() const override { return RTAnyType::kEdge; }

  inline bool has_value(size_t idx) const {
    auto& e = edges_[idx];
    return std::get<0>(e) != std::numeric_limits<vid_t>::max() &&
           std::get<1>(e) != std::numeric_limits<vid_t>::max();
  }
  inline bool full() const {
    return size_ == gs::chunked_runtime::Configs::CHUNK_SIZE;
  }
  inline bool is_optional() const { return is_optional_; }

  inline void clear() override {
    size_ = 0;
    is_optional_ = false;
    prop_col_.clear();
  }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  Direction dir() const override { return Direction::kBoth; }

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      auto& e = edges_[i];
      auto src = std::get<0>(e);
      auto dst = std::get<1>(e);
      auto dir = std::get<2>(e);
      func(i, label_, src, dst, prop_col_.get(i),
           (dir ? Direction::kOut : Direction::kIn));
    }
  }

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func,
                           const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      int len = offsets[i] & 0xFFFFFFFF;
      int offset = offsets[i] >> 32;
      for (int j = 0; j < len; ++j) {
        size_t idx = offset + j;
        auto& e = edges_[idx];
        auto src = std::get<0>(e);
        auto dst = std::get<1>(e);
        auto dir = std::get<2>(e);
        size_t index = (i << 32) | idx;
        func(index, label_, src, dst, prop_col_.get(idx),
             (dir ? Direction::kOut : Direction::kIn));
      }
    }
  }
  bool is_optional_;
  uint32_t size_;
  LabelTriplet label_;
  PropertyType prop_type_;
  edge_property_vec prop_col_;
  std::unique_ptr<std::tuple<vid_t, vid_t, bool>[]> edges_;
};

class BDMLEdgeColumn : public IEdgeColumn {
 public:
  BDMLEdgeColumn(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : size_(0), is_optional_(false) {
    edges_ = std::make_unique<std::tuple<label_t, vid_t, vid_t, int, bool>[]>(
        gs::chunked_runtime::Configs::CHUNK_SIZE);
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      edge_labels_.emplace_back(label);
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          edge_property_vec::create_edge_prop_vec(label.second);
    }
  }

  inline EdgeRecord get_edge(size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = edge_labels_[index].first;
    auto offset = std::get<3>(e);
    EdgeRecord ret;
    ret.label_triplet_ = label;
    ret.src_ = std::get<1>(e);
    ret.dst_ = std::get<2>(e);
    prop_cols_[index].get_edge_data(ret.prop_, offset);

    ret.dir_ = (std::get<4>(e) ? Direction::kOut : Direction::kIn);
    return ret;
  }

  inline size_t size() const override { return size_; }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < edge_labels_.size(); ++idx) {
      auto label = edge_labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "BDMLEdgeColumn: label = {" + ss.str() +
           "}, size = " + std::to_string(size_);
  }

  std::vector<LabelTriplet> get_labels() const override {
    std::vector<LabelTriplet> labels;
    for (auto& label : edge_labels_) {
      labels.push_back(label.first);
    }
    return labels;
  }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDML;
  }

  inline void push_back_opt(LabelTriplet label, vid_t src, vid_t dst,
                            const EdgeData& data, Direction dir) {
    auto index = index_[label];
    size_t len = prop_cols_[index].size();
    prop_cols_[index].set_edge_data(data, len);
    edges_[size_++] =
        std::make_tuple(index, src, dst, len, dir == Direction::kOut);
  }

  inline void push_back_null() {
    size_t len = prop_cols_[0].size();
    is_optional_ = true;
    prop_cols_[0].set_null(len);
    edges_[size_++] =
        std::make_tuple(0, std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), len, false);
  }

  inline bool full() const {
    return size_ == gs::chunked_runtime::Configs::CHUNK_SIZE;
  }
  inline bool is_optional() const { return is_optional_; }
  inline bool has_value(size_t idx) const {
    auto& e = edges_[idx];
    return std::get<1>(e) != std::numeric_limits<vid_t>::max() &&
           std::get<2>(e) != std::numeric_limits<vid_t>::max();
  }

  inline void clear() override {
    size_ = 0;
    is_optional_ = false;
    for (auto& col : prop_cols_) {
      col.clear();
    }
  }

  Direction dir() const override { return Direction::kBoth; }

  std::shared_ptr<IContextColumn> shuffle(const ValueColumn<size_t>& offsets,
                                          bool shift) override;

  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func) const {
    for (size_t i = 0; i < size_; ++i) {
      auto& e = edges_[i];
      auto index = std::get<0>(e);
      auto label = edge_labels_[index].first;
      auto src = std::get<1>(e);
      auto dst = std::get<2>(e);
      auto offset = std::get<3>(e);
      auto dir = std::get<4>(e);
      func(i, label, src, dst, prop_cols_[index].get(offset),
           (dir ? Direction::kOut : Direction::kIn));
    }
  }
  template <typename FUNC_T>
  inline void foreach_edge(const FUNC_T& func,
                           const ValueColumn<size_t>& offsets) const {
    size_t sz = offsets.size();
    for (size_t i = 0; i < sz; ++i) {
      int len = offsets[i] & 0xFFFFFFFF;
      int offset = offsets[i] >> 32;
      for (int j = 0; j < len; ++j) {
        size_t idx = offset + j;
        auto& e = edges_[idx];
        auto index = std::get<0>(e);
        auto label = edge_labels_[index].first;
        auto src = std::get<1>(e);
        auto dst = std::get<2>(e);
        auto offset = std::get<3>(e);
        auto dir = std::get<4>(e);
        size_t index2 = (i << 32) | idx;
        func(index2, label, src, dst, prop_cols_[index].get(offset),
             (dir ? Direction::kOut : Direction::kIn));
      }
    }
  }

  uint32_t size_;
  bool is_optional_;
  std::unordered_map<LabelTriplet, label_t, LabelTripletHash> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::unique_ptr<std::tuple<label_t, vid_t, vid_t, int, bool>[]> edges_;
  std::vector<edge_property_vec> prop_cols_;
};

template <typename FUNC_T>
inline void IEdgeColumn::foreach_edge(const FUNC_T& func) const {
  if (edge_column_type() == EdgeColumnType::kSDSL) {
    static_cast<const SDSLEdgeColumn*>(this)->foreach_edge(func);
  } else if (edge_column_type() == EdgeColumnType::kSDML) {
    static_cast<const SDMLEdgeColumn*>(this)->foreach_edge(func);
  } else if (edge_column_type() == EdgeColumnType::kBDSL) {
    static_cast<const BDSLEdgeColumn*>(this)->foreach_edge(func);
  } else if (edge_column_type() == EdgeColumnType::kBDML) {
    static_cast<const BDMLEdgeColumn*>(this)->foreach_edge(func);
  } else {
    LOG(FATAL) << "Unknown edge column type: "
               << static_cast<int>(edge_column_type());
  }
}

template <typename FUNC_T>
inline void IEdgeColumn::foreach_edge(
    const FUNC_T& func, const ValueColumn<size_t>& offsets) const {
  if (edge_column_type() == EdgeColumnType::kSDSL) {
    static_cast<const SDSLEdgeColumn*>(this)->foreach_edge(func, offsets);
  } else if (edge_column_type() == EdgeColumnType::kSDML) {
    static_cast<const SDMLEdgeColumn*>(this)->foreach_edge(func, offsets);
  } else if (edge_column_type() == EdgeColumnType::kBDSL) {
    static_cast<const BDSLEdgeColumn*>(this)->foreach_edge(func, offsets);
  } else if (edge_column_type() == EdgeColumnType::kBDML) {
    static_cast<const BDMLEdgeColumn*>(this)->foreach_edge(func, offsets);
  } else {
    LOG(FATAL) << "Unknown edge column type: "
               << static_cast<int>(edge_column_type());
  }
}

}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_COMMON_DATA_CHUNKS_EDGE_COLUMNS_H_