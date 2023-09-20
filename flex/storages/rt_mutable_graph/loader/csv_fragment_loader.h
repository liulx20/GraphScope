
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"

namespace gs {

// LoadFragment for csv files.
class CSVFragmentLoader : public IFragmentLoader {
 public:
  CSVFragmentLoader(const std::string& work_dir, const Schema& schema,
                    const LoadingConfig& loading_config, int32_t thread_num)
      : loading_config_(loading_config),
        schema_(schema),
        work_dir_(work_dir),
        thread_num_(thread_num),
        basic_fragment_loader_(schema_, work_dir) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
  }

  ~CSVFragmentLoader() {}

  FragmentLoaderType GetFragmentLoaderType() const override {
    return FragmentLoaderType::kCSVFragmentLoader;
  }

  void LoadFragment(MutablePropertyFragment& fragment) override;

 private:
  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  void addVerticesImpl(label_t v_label_id, const std::string& v_label_name,
                       const std::vector<std::string> v_file,
                       IdIndexer<oid_t, vid_t>& indexer);

  void addVertexBatch(
      label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  template <typename EDATA_T>
  void addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                    label_t e_label_id,
                    const std::vector<std::string>& e_files);

  const LoadingConfig& loading_config_;
  const Schema& schema_;
  std::string work_dir_;
  size_t vertex_label_num_, edge_label_num_;
  int32_t thread_num_;

  mutable BasicFragmentLoader basic_fragment_loader_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_