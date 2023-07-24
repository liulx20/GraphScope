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

#ifndef GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_
#define GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_

#include <stdio.h>

#include <grape/serialization/in_archive.h>
#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/utils/allocators.h"

namespace gs {

class DualCsrBase {
 public:
  DualCsrBase() = default;
  virtual ~DualCsrBase() = default;

  virtual void ConstructEmptyCsr() = 0;
  virtual void BulkLoad(const LFIndexer<vid_t>& src_indexer,
                        const LFIndexer<vid_t>& dst_indexer,
                        const std::vector<std::string>& filenames) = 0;

  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp, ArenaAllocator& alloc) = 0;
  virtual void PutEdge(vid_t src, vid_t dst, timestamp_t timestamp,
                       const Property& data, ArenaAllocator& alloc) = 0;
  virtual MutableCsrBase* GetInCsr() = 0;
  virtual MutableCsrBase* GetOutCsr() = 0;

  virtual void Serialize(const std::string& path) = 0;
  virtual void Deserialize(const std::string& path) = 0;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {}

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Property& data,
                        timestamp_t ts, ArenaAllocator& alloc) override {}

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& alloc) override {}
  void Serialize(const std::string& path) override {}

  void Deserialize(const std::string& path) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, ArenaAllocator& alloc) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
};
/**
template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {}

  void Serialize(const std::string& path) override {}

  void Deserialize(const std::string& path) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& alloc) override {}
};
*/
inline void preprocess_line(char* line) {
  size_t len = strlen(line);
  while (len >= 0) {
    if (line[len] != '\0' && line[len] != '\n' && line[len] != '\r' &&
        line[len] != ' ' && line[len] != '\t') {
      break;
    } else {
      --len;
    }
  }
  line[len + 1] = '\0';
}

template <typename EDATA_T>
class DualTypedCsr : public DualCsrBase {
 public:
  DualTypedCsr(EdgeStrategy ie_strategy, EdgeStrategy oe_strategy,
               const std::vector<PropertyType>& properties)
      : in_csr_(nullptr), out_csr_(nullptr), properties_(properties) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      in_csr_ = new MutableCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      in_csr_ = new SingleMutableCsr<EDATA_T>();
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      out_csr_ = new MutableCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      out_csr_ = new SingleMutableCsr<EDATA_T>();
    }
  }

  ~DualTypedCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }

  void ConstructEmptyCsr() override {
    in_csr_->batch_init(0, {});
    out_csr_->batch_init(0, {});
  }

  void BulkLoad(const LFIndexer<vid_t>& src_indexer,
                const LFIndexer<vid_t>& dst_indexer,
                const std::vector<std::string>& filenames) override {
    std::vector<int> odegree(src_indexer.size(), 0);
    std::vector<int> idegree(dst_indexer.size(), 0);

    std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
    vid_t src_index, dst_index;
    char line_buf[4096];
    oid_t src, dst;
    EDATA_T data;

    bool first_file = true;
    size_t col_num = properties_.size();
    std::vector<Property> header(col_num + 2);
    for (auto& item : header) {
      item.set_type(PropertyType::kString);
    }
    for (auto filename : filenames) {
      FILE* fin = fopen(filename.c_str(), "r");
      if (fgets(line_buf, 4096, fin) == NULL) {
        continue;
      }
      preprocess_line(line_buf);
      if (first_file) {
        ParseRecord(line_buf, header);
        std::vector<std::string> col_names(col_num);
        for (size_t i = 0; i < col_num; ++i) {
          col_names[i] = header[i + 2].get_value<std::string>();
        }
        first_file = false;
      }

      while (fgets(line_buf, 4096, fin) != NULL) {
        ParseRecordX(line_buf, src, dst, std::ref(data));
        src_index = src_indexer.get_index(src);
        dst_index = dst_indexer.get_index(dst);
        ++idegree[dst_index];
        ++odegree[src_index];
        parsed_edges.emplace_back(src_index, dst_index, data);
      }
      fclose(fin);
    }
    in_csr_->batch_init(dst_indexer.size(), idegree);
    out_csr_->batch_init(src_indexer.size(), odegree);

    for (auto& edge : parsed_edges) {
      in_csr_->batch_put_edge(std::get<1>(edge), std::get<0>(edge),
                              std::get<2>(edge));
      out_csr_->batch_put_edge(std::get<0>(edge), std::get<1>(edge),
                               std::get<2>(edge));
    }
  }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                  timestamp_t timestamp, ArenaAllocator& alloc) override {
    EDATA_T data;
    oarc >> data;
    in_csr_->put_edge(dst, src, data, timestamp, alloc);
    out_csr_->put_edge(src, dst, data, timestamp, alloc);
  }
  void PutEdge(vid_t src, vid_t dst, timestamp_t timestamp,
               const Property& prop, ArenaAllocator& alloc) override {
    EDATA_T data = prop.get_value<EDATA_T>();
    in_csr_->put_edge(dst, src, data, timestamp, alloc);
    out_csr_->put_edge(src, dst, data, timestamp, alloc);
  }
  MutableCsrBase* GetInCsr() override { return in_csr_; }
  MutableCsrBase* GetOutCsr() override { return out_csr_; }

  void Serialize(const std::string& path) override {
    in_csr_->Serialize(path + "_ie");
    out_csr_->Serialize(path + "_oe");
  }
  void Deserialize(const std::string& path) override {
    in_csr_->Deserialize(path + "_ie");
    out_csr_->Deserialize(path + "_oe");
  }

 private:
  TypedMutableCsrBase<EDATA_T>* in_csr_;
  TypedMutableCsrBase<EDATA_T>* out_csr_;
  std::vector<PropertyType> properties_;
};

class DualTableCsr : public DualCsrBase {
 public:
  DualTableCsr(const std::vector<PropertyType>& properties) {
    std::vector<std::string> col_names;
    std::vector<StorageStrategy> col_strategies;
    size_t col_num = properties.size();
    for (size_t i = 0; i < col_num; ++i) {
      col_names.push_back("col_" + std::to_string(i));
      col_strategies.push_back(StorageStrategy::kMem);
    }
    table_.init(col_names, properties, col_strategies,
                std::numeric_limits<int>::max());
    in_csr_.set_table(&table_);
    out_csr_.set_table(&table_);
    table_index_.store(0);
    properties_ = properties;
  }

  ~DualTableCsr() {}

  void ConstructEmptyCsr() override {
    in_csr_.batch_init(0, {});
    out_csr_.batch_init(0, {});
  }

  void BulkLoad(const LFIndexer<vid_t>& src_indexer,
                const LFIndexer<vid_t>& dst_indexer,
                const std::vector<std::string>& filenames) override {
    std::vector<int> odegree(src_indexer.size(), 0);
    std::vector<int> idegree(dst_indexer.size(), 0);

    std::vector<std::tuple<vid_t, vid_t, size_t>> parsed_edges;
    vid_t src_index, dst_index;
    char line_buf[4096];
    oid_t src, dst;
    std::vector<Property> data(properties_.size());

    bool first_file = true;
    size_t col_num = properties_.size();
    std::vector<Property> header(col_num + 2);
    for (auto& item : header) {
      item.set_type(PropertyType::kString);
    }
    for (size_t col_i = 0; col_i != col_num; ++col_i) {
      data[col_i].set_type(properties_[col_i] == PropertyType::kString
                               ? PropertyType::kStringView
                               : properties_[col_i]);
    }
    for (auto filename : filenames) {
      FILE* fin = fopen(filename.c_str(), "r");
      if (fgets(line_buf, 4096, fin) == NULL) {
        continue;
      }
      preprocess_line(line_buf);
      if (first_file) {
        ParseRecord(line_buf, header);
        std::vector<std::string> col_names(col_num);
        for (size_t i = 0; i < col_num; ++i) {
          col_names[i] = header[i + 2].get_value<std::string>();
        }
        table_.reset_header(col_names);
        first_file = false;
      }

      while (fgets(line_buf, 4096, fin) != NULL) {
        ParseRecordX(line_buf, src, dst, data);
        src_index = src_indexer.get_index(src);
        dst_index = dst_indexer.get_index(dst);
        ++idegree[dst_index];
        ++odegree[src_index];
        size_t row_id = table_index_.fetch_add(1);
        table_.insert(row_id, data);
        parsed_edges.emplace_back(src_index, dst_index, row_id);
      }
      fclose(fin);
    }

    in_csr_.batch_init(dst_indexer.size(), idegree);
    out_csr_.batch_init(src_indexer.size(), odegree);

    for (auto& edge : parsed_edges) {
      in_csr_.batch_put_edge_with_index(std::get<1>(edge), std::get<0>(edge),
                                        std::get<2>(edge), 0);
      out_csr_.batch_put_edge_with_index(std::get<0>(edge), std::get<1>(edge),
                                         std::get<2>(edge), 0);
    }
  }

  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp,
                          ArenaAllocator& alloc) override {
    std::vector<Property> props;
    oarc >> props;
    size_t row_id = table_index_.fetch_add(1);
    table_.insert(row_id, props);
    in_csr_.put_edge_with_index(dst, src, row_id, timestamp, alloc);
    out_csr_.put_edge_with_index(src, dst, row_id, timestamp, alloc);
  }
  virtual void PutEdge(vid_t src, vid_t dst, timestamp_t timestamp,
                       const Property& prop, ArenaAllocator& alloc) override {
    std::vector<Property> props = prop.get_value<std::vector<Property>>();
    size_t row_id = table_index_.fetch_add(1);
    table_.insert(row_id, props);
    in_csr_.put_edge_with_index(dst, src, row_id, timestamp, alloc);
    out_csr_.put_edge_with_index(src, dst, row_id, timestamp, alloc);
  }

  MutableCsrBase* GetInCsr() override { return &in_csr_; }
  MutableCsrBase* GetOutCsr() override { return &out_csr_; }

  void Serialize(const std::string& path) override {
    std::string table_index_path = path + ".table_index";
    FILE* fout = fopen(table_index_path.c_str(), "wb");
    fwrite(&table_index_, sizeof(table_index_), 1, fout);
    fclose(fout);

    table_.Serialize(path + "_etable", table_index_.load());
    in_csr_.Serialize(path + "_ie");
    out_csr_.Serialize(path + "_oe");
  }
  void Deserialize(const std::string& path) override {
    std::string table_index_path = path + ".table_index";
    FILE* fin = fopen(table_index_path.c_str(), "r");
    fread(&table_index_, sizeof(table_index_), 1, fin);
    fclose(fin);

    table_.Deserialize(path + "_etable");
    in_csr_.Deserialize(path + "_ie");
    out_csr_.Deserialize(path + "_oe");
  }

 protected:
  TableMutableCsr in_csr_;
  TableMutableCsr out_csr_;
  Table table_;
  std::atomic<size_t> table_index_;

  std::vector<PropertyType> properties_;
};
/**
class DualStringCsr : public DualCsrBase {
 public:
  DualStringCsr() : column_(StorageStrategy::kMem) {
    column_.init(std::numeric_limits<int>::max());
    in_csr_.set_column(&column_);
    out_csr_.set_column(&column_);
    table_index_.store(0);
  }
  ~DualStringCsr() {}

  void ConstructEmptyCsr() override {
    in_csr_.batch_init(0, {});
    out_csr_.batch_init(0, {});
  }

  void BulkLoad(const LFIndexer<vid_t>& src_indexer,
                const LFIndexer<vid_t>& dst_indexer,
                const std::vector<std::string>& filenames) override {
    std::vector<int> odegree(src_indexer.size(), 0);
    std::vector<int> idegree(dst_indexer.size(), 0);

    std::vector<std::tuple<vid_t, vid_t, size_t>> parsed_edges;
    vid_t src_index, dst_index;
    char line_buf[4096];
    oid_t src, dst;
    std::string_view data;

    for (auto filename : filenames) {
      FILE* fin = fopen(filename.c_str(), "r");
      if (fgets(line_buf, 4096, fin) == NULL) {
        continue;
      }
      preprocess_line(line_buf);

      while (fgets(line_buf, 4096, fin) != NULL) {
        ParseRecordX(line_buf, src, dst, data);
        src_index = src_indexer.get_index(src);
        dst_index = dst_indexer.get_index(dst);
        ++idegree[dst_index];
        ++odegree[src_index];
        size_t row_id = table_index_.fetch_add(1);
        column_.set_value(row_id, data);
        parsed_edges.emplace_back(src_index, dst_index, row_id);
      }
      fclose(fin);
    }

    in_csr_.batch_init(dst_indexer.size(), idegree);
    out_csr_.batch_init(src_indexer.size(), odegree);

    for (auto& edge : parsed_edges) {
      in_csr_.batch_put_edge_with_index(std::get<1>(edge), std::get<0>(edge),
                                        std::get<2>(edge), 0);
      out_csr_.batch_put_edge_with_index(std::get<0>(edge), std::get<1>(edge),
                                         std::get<2>(edge), 0);
    }
  }

  virtual void IngestEdge(vid_t src, vid_t dst,
                          grape::OutArchive& oarc,timestamp_t timestamp,
                          ArenaAllocator& alloc) override {
    std::string_view prop;
    oarc >> prop;
    size_t row_id = table_index_.fetch_add(1);
    column_.set_value(row_id, prop);
    in_csr_.put_edge_with_index(dst, src, row_id, timestamp, alloc);
    out_csr_.put_edge_with_index(src, dst, row_id, timestamp, alloc);
  }

  virtual void PutEdge(vid_t src, vid_t dst, timestamp_t timestamp,
                       const Property& prop, ArenaAllocator& alloc) override {
    size_t row_id = table_index_.fetch_add(1);
    if (prop.type() == PropertyType::kString) {
      column_.set_value(row_id, prop.get_value<std::string>());
    } else if (prop.type() == PropertyType::kStringView) {
      column_.set_value(row_id, prop.get_value<std::string_view>());
    } else {
      LOG(FATAL) << "Unexpected property type: " << prop.type()
                 << ", string or string_view is expected...";
    }

    in_csr_.put_edge_with_index(dst, src, row_id, timestamp, alloc);
    out_csr_.put_edge_with_index(src, dst, row_id, timestamp, alloc);
  }

  MutableCsrBase* GetInCsr() override { return &in_csr_; }
  MutableCsrBase* GetOutCsr() override { return &out_csr_; }

  void Serialize(const std::string& path) override {
    std::string table_index_path = path + ".table_index";
    FILE* fout = fopen(table_index_path.c_str(), "wb");
    fwrite(&table_index_, sizeof(table_index_), 1, fout);
    fclose(fout);

    column_.Serialize(path + "_ecolumn", table_index_.load());
    in_csr_.Serialize(path + "_ie");
    out_csr_.Serialize(path + "_oe");
  }
  void Deserialize(const std::string& path) override {
    std::string table_index_path = path + ".table_index";
    FILE* fin = fopen(table_index_path.c_str(), "r");
    fread(&table_index_, sizeof(table_index_), 1, fin);
    fclose(fin);

    column_.Deserialize(path + "_ecolumn");
    in_csr_.Deserialize(path + "_ie");
    out_csr_.Deserialize(path + "_oe");
  }

 private:
  MutableCsr<std::string> in_csr_;
  MutableCsr<std::string> out_csr_;
  StringColumn column_;
  std::atomic<size_t> table_index_;
};
*/

inline DualCsrBase* create_dual_csr(
    EdgeStrategy ies, EdgeStrategy oes,
    const std::vector<PropertyType>& properties) {
  if (properties.empty()) {
    return new DualTypedCsr<grape::EmptyType>(ies, oes, properties);
  } /**else if (properties.size() == 1) {
    switch (properties[0]) {
    case PropertyType::kInt32:
      return new DualTypedCsr<int32_t>(ies, oes, properties);
    case PropertyType::kDate:
      return new DualTypedCsr<Date>(ies, oes, properties);
    case PropertyType::kInt64:
      return new DualTypedCsr<int64_t>(ies, oes, properties);
    case PropertyType::kString:
    case PropertyType::kStringView:
      return new DualTypedCsr<std::string>(ies, oes, properties);
    case PropertyType::kDouble:
      return new DualTypedCsr<double>(ies, oes, properties);
    default:
      LOG(FATAL) << "Unsupported property type - " << properties[0];
      return nullptr;
    }
  } */
  else {
    return new DualTableCsr(properties);
  }
}

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_
