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

#ifndef CHUNKED_RUNTIME_COMMON_ACCESSORS_H_
#define CHUNKED_RUNTIME_COMMON_ACCESSORS_H_

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunk.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/edge_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/path_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/value_columns.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/vertex_columns.h"
#include "flex/engines/graph_db/database/read_transaction.h"

#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace chunked_runtime {

class IAccessor {
 public:
  virtual ~IAccessor() = default;
  virtual RTAny eval_path(size_t idx) const {
    LOG(FATAL) << "eval_path not implemented";
    return RTAny(RTAnyType::kNull);
  }
  virtual RTAny eval_vertex(label_t label, vid_t v) const {
    LOG(FATAL) << "eval_vertex not implemented";
    return RTAny(RTAnyType::kNull);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data) const {
    LOG(FATAL) << "eval_edge not implemented";
    return RTAny(RTAnyType::kNull);
  }

  virtual RTAny eval_path(size_t idx, int) const {
    return this->eval_path(idx);
  }
  virtual RTAny eval_vertex(label_t label, vid_t v, int) const {
    return this->eval_vertex(label, v);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, int) const {
    return this->eval_edge(label, src, dst, data);
  }

  virtual bool is_optional() const { return false; }

  virtual std::string name() const { return "unknown"; }
};

class VertexPathAccessor : public IAccessor {
 public:
  using elem_t = VertexRecord;

  VertexPathAccessor(const DataChunk& ctx, int tag) {
    auto ptr = ctx.get(tag);
    if (dynamic_cast<const SLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const SLVertexColumn*>(ptr);
    } else if (dynamic_cast<const MLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const MLVertexColumn*>(ptr);
    } else {
      LOG(FATAL) << "Invalid vertex column type";
    }
  }

  bool is_optional() const override {
    return std::visit([](const auto& col) { return col->is_optional(); },
                      vertex_col_);
  }

  elem_t typed_eval_path(size_t idx) const {
    return std::visit([idx](const auto& col) { return col->get_vertex(idx); },
                      vertex_col_);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_vertex(typed_eval_path(idx));
  }

  RTAny eval_path(size_t idx, int) const override {
    return std::visit(
        [this, idx](const auto& col) {
          if (!col->has_value(idx)) {
            return RTAny(RTAnyType::kNull);
          }
          return RTAny::from_vertex(typed_eval_path(idx));
        },
        vertex_col_);
  }

 private:
  std::variant<const SLVertexColumn*, const MLVertexColumn*> vertex_col_;
};

class VertexGIdPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdPathAccessor(const DataChunk& ctx, int tag) {
    auto ptr = ctx.get(tag);
    if (dynamic_cast<const SLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const SLVertexColumn*>(ptr);
    } else if (dynamic_cast<const MLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const MLVertexColumn*>(ptr);
    } else {
      LOG(FATAL) << "Invalid vertex column type";
    }
  }

  bool is_optional() const override {
    return std::visit([](const auto& col) { return col->is_optional(); },
                      vertex_col_);
  }

  elem_t typed_eval_path(size_t idx) const {
    return std::visit(
        [idx](const auto& col) {
          const auto& v = col->get_vertex(idx);
          return gs::runtime::encode_unique_vertex_id(v.label_, v.vid_);
        },
        vertex_col_);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_int64(typed_eval_path(idx));
  }

 private:
  std::variant<const SLVertexColumn*, const MLVertexColumn*> vertex_col_;
};

template <typename GraphInterface, typename T>
class VertexPropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  VertexPropertyPathAccessor(const GraphInterface& graph, const DataChunk& ctx,
                             int tag, const std::string& prop_name)
      : is_optional_(false) {
    auto ptr = dynamic_cast<const IVertexColumn*>(ctx.get(tag));
    if (!ptr) {
      LOG(FATAL) << "Invalid vertex column type";
    }
    if (dynamic_cast<const SLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const SLVertexColumn*>(ptr);
      is_optional_ = dynamic_cast<const SLVertexColumn*>(ptr)->is_optional();
    } else if (dynamic_cast<const MLVertexColumn*>(ptr)) {
      vertex_col_ = dynamic_cast<const MLVertexColumn*>(ptr);
      is_optional_ = dynamic_cast<const MLVertexColumn*>(ptr)->is_optional();
    } else {
      LOG(FATAL) << "Invalid vertex column type";
    }

    int label_num = graph.schema().vertex_label_num();
    property_columns_.resize(label_num);
    const auto& labels = std::visit(
        [](const auto& col) { return col->get_labels_set(); }, vertex_col_);

    for (auto label : labels) {
      property_columns_[label] =
          graph.template GetVertexColumn<T>(label, prop_name);
      if (property_columns_[label].is_null()) {
        is_optional_ = true;
      }
    }
  }

  bool is_optional() const override { return is_optional_; }

  elem_t typed_eval_path(size_t idx) const {
    return std::visit(
        [this, idx](const auto& col) {
          const auto& v = col->get_vertex(idx);
          auto prop_col = property_columns_[v.label_];
          assert(!prop_col.is_null());
          return prop_col.get_view(v.vid_);
        },
        vertex_col_);
  }

  RTAny eval_path(size_t idx) const override {
    auto val = gs::runtime::TypedConverter<T>::from_typed(typed_eval_path(idx));
    return val;
  }

  RTAny eval_path(size_t idx, int) const override {
    return std::visit(
        [this, idx](const auto& col) {
          if (!col->has_value(idx)) {
            return RTAny(RTAnyType::kNull);
          }
          const auto& v = col->get_vertex(idx);
          auto& prop_col = property_columns_[v.label_];
          if (!prop_col.is_null()) {
            return gs::runtime::TypedConverter<T>::from_typed(
                prop_col.get_view(v.vid_));
          } else {
            return RTAny(RTAnyType::kNull);
          }
        },
        vertex_col_);
  }

 private:
  bool is_optional_;
  std::variant<const SLVertexColumn*, const MLVertexColumn*> vertex_col_;
  using vertex_column_t =
      typename GraphInterface::template vertex_column_t<elem_t>;
  std::vector<vertex_column_t> property_columns_;
};

class VertexLabelPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  VertexLabelPathAccessor(const DataChunk& ctx, int tag)
      : vertex_col_(*dynamic_cast<const IVertexColumn*>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return static_cast<int32_t>(vertex_col_.get_vertex(idx).label_);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny(static_cast<int32_t>(typed_eval_path(idx)));
  }

 private:
  const IVertexColumn& vertex_col_;
};

class VertexLabelVertexAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexLabelVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    return static_cast<int64_t>(label);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    return RTAny::from_int64(label);
  }
};
template <typename T>
class ContextValueAccessor : public IAccessor {
 public:
  using elem_t = T;
  ContextValueAccessor(const DataChunk& ctx, int tag)
      : col_(*dynamic_cast<const ValueColumn<elem_t>*>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return col_.get_value(idx); }

  RTAny eval_path(size_t idx) const override { return col_.get_elem(idx); }

  bool is_optional() const override { return col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

 private:
  const ValueColumn<elem_t>& col_;
};

class VertexIdVertexAccessor : public IAccessor {
 public:
  using elem_t = VertexRecord;
  VertexIdVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    return VertexRecord{label, v};
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    return RTAny::from_vertex(typed_eval_vertex(label, v));
  }

  RTAny eval_vertex(label_t label, vid_t v, int) const override {
    if (v == std::numeric_limits<vid_t>::max()) {
      return RTAny(RTAnyType::kNull);
    }
    return RTAny::from_vertex(typed_eval_vertex(label, v));
  }
};

class VertexGIdVertexAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    return gs::runtime::encode_unique_vertex_id(label, v);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    return RTAny::from_int64(typed_eval_vertex(label, v));
  }
};

template <typename GraphInterface, typename T>
class VertexPropertyVertexAccessor : public IAccessor {
 public:
  using elem_t = T;

  VertexPropertyVertexAccessor(const GraphInterface& graph,
                               const std::string& prop_name) {
    int label_num = graph.schema().vertex_label_num();
    for (int i = 0; i < label_num; ++i) {
      property_columns_.emplace_back(graph.template GetVertexColumn<T>(
          static_cast<label_t>(i), prop_name));
    }
  }

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    if (property_columns_[label].is_null()) {
      return elem_t();
    }
    return property_columns_[label].get_view(v);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    if (property_columns_[label].is_null()) {
      return RTAny();
    }
    return gs::runtime::TypedConverter<T>::from_typed(
        property_columns_[label].get_view(v));
  }

  RTAny eval_vertex(label_t label, vid_t v, int) const override {
    if (property_columns_[label].is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return gs::runtime::TypedConverter<T>::from_typed(
        property_columns_[label].get_view(v));
  }

  bool is_optional() const override {
    for (auto col : property_columns_) {
      if (col.is_null()) {
        return true;
      }
    }
    return false;
  }

 private:
  using vertex_column_t =
      typename GraphInterface::template vertex_column_t<elem_t>;
  std::vector<vertex_column_t> property_columns_;
};

class EdgeIdPathAccessor : public IAccessor {
 public:
  using elem_t = EdgeRecord;
  EdgeIdPathAccessor(const DataChunk& ctx, int tag)
      : edge_col_(*dynamic_cast<const IEdgeColumn*>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return edge_col_.get_edge(idx); }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_edge(typed_eval_path(idx));
  }

  bool is_optional() const override { return edge_col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!edge_col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return RTAny::from_edge(typed_eval_path(idx));
  }

 private:
  const IEdgeColumn& edge_col_;
};

template <typename GraphInterface, typename T>
class EdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyPathAccessor(const GraphInterface& graph,
                           const std::string& prop_name, const DataChunk& ctx,
                           int tag)
      : col_(*dynamic_cast<const IEdgeColumn*>(ctx.get(tag))) {}

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    return RTAny(e.prop_);
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    elem_t ret = e.prop_.as<elem_t>();
    return ret;
  }

  bool is_optional() const override { return col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

 private:
  const IEdgeColumn& col_;
};

template <typename GraphInterface, typename T>
class MultiPropsEdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  MultiPropsEdgePropertyPathAccessor(const GraphInterface& graph,
                                     const std::string& prop_name,
                                     const DataChunk& ctx, int tag)
      : col_(*dynamic_cast<const IEdgeColumn*>(ctx.get(tag))) {
    const auto& labels = col_.get_labels();
    vertex_label_num_ = graph.schema().vertex_label_num();
    edge_label_num_ = graph.schema().edge_label_num();
    prop_index_.resize(
        2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_,
        std::numeric_limits<size_t>::max());
    for (auto& label : labels) {
      size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                   label.dst_label * edge_label_num_ + label.edge_label;
      const auto& names = graph.schema().get_edge_property_names(
          label.src_label, label.dst_label, label.edge_label);
      for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == prop_name) {
          prop_index_[idx] = i;
          break;
        }
      }
    }
  }

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    auto val = e.prop_;
    auto id = get_index(e.label_triplet_);
    if (e.prop_.type != RTAnyType::kRecordView) {
      assert(id == 0);
      return RTAny(val);
    } else {
      auto rv = val.as<RecordView>();
      assert(id != std::numeric_limits<size_t>::max());
      return RTAny(rv[id]);
    }
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    auto val = e.prop_;
    auto id = get_index(e.label_triplet_);

    if (e.prop_.type != RTAnyType::kRecordView) {
      assert(id == 0);
      elem_t ret = e.prop_.as<elem_t>();

      return ret;

    } else {
      auto rv = val.as<RecordView>();
      assert(id != std::numeric_limits<size_t>::max());
      auto tmp = rv[id];
      elem_t ret;
      ConvertAny<T>::to(tmp, ret);
      return ret;
    }
  }

  bool is_optional() const override { return col_.is_optional(); }

  size_t get_index(const LabelTriplet& label) const {
    size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                 label.dst_label * edge_label_num_ + label.edge_label;
    return prop_index_[idx];
  }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

 private:
  const IEdgeColumn& col_;
  std::vector<size_t> prop_index_;
  size_t vertex_label_num_;
  size_t edge_label_num_;
};

class EdgeLabelPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  EdgeLabelPathAccessor(const DataChunk& ctx, int tag)
      : col_(*dynamic_cast<const IEdgeColumn*>(ctx.get(tag))) {}

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    return RTAny(static_cast<int32_t>(e.label_triplet_.edge_label));
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    return static_cast<int32_t>(e.label_triplet_.edge_label);
  }

 private:
  const IEdgeColumn& col_;
};

template <typename GraphInterface, typename T>
class EdgePropertyEdgeAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyEdgeAccessor(const GraphInterface& graph,
                           const std::string& name) {}

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data) const {
    T ret;
    ConvertAny<T>::to(data, ret);
    return ret;
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data) const override {
    return RTAny(data);
  }
};

template <typename GraphInterface, typename T>
class MultiPropsEdgePropertyEdgeAccessor : public IAccessor {
 public:
  using elem_t = T;

  MultiPropsEdgePropertyEdgeAccessor(const GraphInterface& graph,
                                     const std::string& name) {
    edge_label_num_ = graph.schema().edge_label_num();
    vertex_label_num_ = graph.schema().vertex_label_num();
    indexs.resize(2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                  std::numeric_limits<size_t>::max());
    for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
      auto src = graph.schema().get_vertex_label_name(src_label);
      for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
        auto dst = graph.schema().get_vertex_label_name(dst_label);
        for (label_t edge_label = 0; edge_label < edge_label_num_;
             ++edge_label) {
          auto edge = graph.schema().get_edge_label_name(edge_label);
          if (!graph.schema().exist(src, dst, edge)) {
            continue;
          }
          size_t idx = src_label * vertex_label_num_ * edge_label_num_ +
                       dst_label * edge_label_num_ + edge_label;
          const std::vector<std::string>& names =
              graph.schema().get_edge_property_names(src_label, dst_label,
                                                     edge_label);
          for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == name) {
              indexs[idx] = i;
              break;
            }
          }
        }
      }
    }
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data) const {
    T ret;
    if (data.type != PropertyType::RecordView()) {
      assert(get_index(label) == 0);
      ConvertAny<T>::to(data, ret);
    } else {
      auto id = get_index(label);
      assert(id != std::numeric_limits<size_t>::max());
      auto view = data.AsRecordView();
      ConvertAny<T>::to(view[id], ret);
    }
    return ret;
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data) const override {
    return RTAny(typed_eval_edge(label, src, dst, data));
  }

  size_t get_index(const LabelTriplet& label) const {
    size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                 label.dst_label * edge_label_num_ + label.edge_label;
    return indexs[idx];
  }

 private:
  std::vector<size_t> indexs;
  size_t vertex_label_num_;
  size_t edge_label_num_;
};

template <typename T>
class ParamAccessor : public IAccessor {
 public:
  using elem_t = T;
  ParamAccessor(const std::map<std::string, std::string>& params,
                const std::string& key) {
    val_ = gs::runtime::TypedConverter<T>::typed_from_string(params.at(key));
  }

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_vertex(label_t, vid_t) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const Any&) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

class PathIdPathAccessor : public IAccessor {
 public:
  using elem_t = Path;
  PathIdPathAccessor(const DataChunk& ctx, int tag)
      : path_col_(*dynamic_cast<const IPathColumn*>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return path_col_.get_path(idx); }

  RTAny eval_path(size_t idx) const override { return path_col_.get_elem(idx); }

 private:
  const IPathColumn& path_col_;
};

class PathLenPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  PathLenPathAccessor(const DataChunk& ctx, int tag)
      : path_col_(*dynamic_cast<const IPathColumn*>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return path_col_.get_path_length(idx);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny(static_cast<int32_t>(typed_eval_path(idx)));
  }

 private:
  const IPathColumn& path_col_;
};

template <typename T>
class ConstAccessor : public IAccessor {
 public:
  using elem_t = T;
  ConstAccessor(const T& val) : val_(val) {}

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_vertex(label_t, vid_t) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const Any&) const override {
    return gs::runtime::TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

std::shared_ptr<IAccessor> create_context_value_accessor(
    const DataChunk& ctx, int tag, gs::runtime::RTAnyType type);

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const GraphInterface& graph, const DataChunk& ctx, int tag,
    gs::runtime::RTAnyType type, const std::string& prop_name);

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const GraphInterface& graph, gs::runtime::RTAnyType type,
    const std::string& prop_name);

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(
    const DataChunk& ctx, int tag);

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const GraphInterface& graph, const std::string& prop_name,
    const DataChunk& ctx, int tag, gs::runtime::RTAnyType type);

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const DataChunk& ctx,
                                                           int tag);

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const GraphInterface& graph, const std::string& prop_name,
    gs::runtime::RTAnyType type);

}  // namespace chunked_runtime

}  // namespace gs

#endif  // CHUNKED_RUNTIME_COMMON_ACCESSORS_H_