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

#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"

namespace gs {

namespace runtime {

std::shared_ptr<IContextColumn> SDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDSLEdgeColumnBuilder builder(dir_, label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  if (prop_type_ == PropertyType::kEmpty) {
    for (auto off : offsets) {
      const auto& e = edges_[off];
      builder.push_back_endpoints(e.first, e.second);
    }
  } else {
    auto& ret_props = *builder.prop_col_;
    ret_props.resize(new_row_num);
    for (size_t idx = 0; idx < new_row_num; ++idx) {
      size_t off = offsets[idx];
      const auto& e = edges_[off];
      builder.push_back_endpoints(e.first, e.second);
      ret_props.set_any(idx, prop_col_.get(), off);
    }
  }

  return builder.finish();
}

std::shared_ptr<IContextColumn> SDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  CHECK(prop_type_ != PropertyType::kRecordView);
  OptionalSDSLEdgeColumnBuilder builder(dir_, label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  if (prop_type_ == PropertyType::kEmpty) {
    for (auto off : offsets) {
      if (off == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
      } else {
        const auto& e = edges_[off];
        builder.push_back_endpoints(e.first, e.second);
      }
    }
  } else {
    auto& ret_props = *builder.prop_col_;
    ret_props.resize(new_row_num);
    for (size_t idx = 0; idx < new_row_num; ++idx) {
      size_t off = offsets[idx];
      if (off == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
      } else {
        const auto& e = edges_[off];
        builder.push_back_endpoints(e.first, e.second);
        ret_props.set_any(idx, prop_col_.get(), off);
      }
    }
  }

  return builder.finish();
}

std::shared_ptr<IContextColumn> SDSLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<SDSLEdgeColumn>(dir_, label_, prop_type_);
  ret->edges_.swap(edges_);
  // shrink to fit
  prop_col_->resize(ret->edges_.size());
  ret->prop_col_ = prop_col_;
  return ret;
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  BDSLEdgeColumnBuilder builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  auto& ret_props = *builder.prop_col_;
  ret_props.resize(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    builder.push_back_endpoints(std::get<0>(e), std::get<1>(e), std::get<2>(e));
    ret_props.set_any(idx, prop_col_.get(), off);
  }

  return builder.finish();
}

std::shared_ptr<IContextColumn> BDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalBDSLEdgeColumnBuilder builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);

  auto& ret_props = *builder.prop_col_;
  ret_props.resize(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      const auto& e = edges_[off];
      builder.push_back_endpoints(std::get<0>(e), std::get<1>(e),
                                  std::get<2>(e));
      ret_props.set_any(idx, prop_col_.get(), off);
    }
  }

  return builder.finish();
}

std::shared_ptr<IContextColumn> BDSLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<BDSLEdgeColumn>(label_, prop_type_);
  prop_col_->resize(edges_.size());
  ret->edges_.swap(edges_);
  ret->prop_col_ = prop_col_;
  return ret;
}

std::shared_ptr<IContextColumn> SDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SDMLEdgeColumnBuilder builder(dir_, edge_labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          prop_cols_[index]->get(offset));
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SDMLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<SDMLEdgeColumn>(dir_, edge_labels_);
  ret->edges_.swap(edges_);
  ret->prop_cols_.swap(prop_cols_);
  return ret;
}

std::shared_ptr<IContextColumn> BDMLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  BDMLEdgeColumnBuilder builder(labels_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = edges_[off];
    auto index = std::get<0>(e);
    auto offset = std::get<3>(e);
    auto dir = std::get<4>(e) ? Direction::kOut : Direction::kIn;
    builder.push_back_opt(index, std::get<1>(e), std::get<2>(e),
                          prop_cols_[index]->get(offset), dir);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> BDMLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<BDMLEdgeColumn>(labels_);
  ret->edges_.swap(edges_);
  ret->prop_cols_.swap(prop_cols_);
  return ret;
}

std::shared_ptr<IContextColumn> OptionalBDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalBDSLEdgeColumnBuilder builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_, e.dir_);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalBDSLEdgeColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalBDSLEdgeColumnBuilder builder(label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    if (off == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
      continue;
    }
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_, e.dir_);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalBDSLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<OptionalBDSLEdgeColumn>(label_, prop_type_);
  ret->edges_.swap(edges_);
  ret->prop_col_ = prop_col_;
  // shrink to fit
  ret->prop_col_->resize(ret->edges_.size());
  return ret;
}

std::shared_ptr<IContextColumn> OptionalSDSLEdgeColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalSDSLEdgeColumnBuilder builder(dir_, label_, prop_type_);
  size_t new_row_num = offsets.size();
  builder.reserve(new_row_num);
  for (size_t idx = 0; idx < new_row_num; ++idx) {
    size_t off = offsets[idx];
    const auto& e = get_edge(off);
    builder.push_back_opt(e.src_, e.dst_, e.prop_);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalSDSLEdgeColumnBuilder::finish() {
  auto ret = std::make_shared<OptionalSDSLEdgeColumn>(dir_, label_, prop_type_);
  ret->edges_.swap(edges_);
  ret->prop_col_ = prop_col_;
  // shrink to fit
  ret->prop_col_->resize(ret->edges_.size());
  return ret;
}

}  // namespace runtime

}  // namespace gs
