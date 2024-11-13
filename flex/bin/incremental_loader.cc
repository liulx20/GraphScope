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

#include <boost/program_options.hpp>

#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/odps_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/schema.h"
// #include "flex/third_party/httplib.h"

#include "grape/serialization/in_archive.h"

namespace gs {

struct TypedVecBase {
  virtual ~TypedVecBase() = default;
  virtual void serialize(grape::InArchive&, size_t) = 0;
  virtual size_t size() const = 0;
  virtual void reserve(size_t) = 0;
};

template <typename T>
struct TypedVec : TypedVecBase {
  std::vector<T> vec;

  TypedVec() = default;
  TypedVec(const std::vector<T>& v) : vec(v) {}

  void serialize(grape::InArchive& arc, size_t idx) override {
    if constexpr (std::is_same_v<T, std::string_view>) {
      arc << std::string(vec[idx]);
    } else if constexpr (std::is_same_v<T, grape::EmptyType>) {
    } else if constexpr (std::is_same_v<T, bool>) {
      if (vec[idx]) {
        arc << true;
      } else {
        arc << false;
      }
    } else {
      arc << vec[idx];
    }
  }

  size_t size() const override { return vec.size(); }
  void reserve(size_t size) { vec.reserve(size); }
  void push_back(const T& val) { vec.push_back(val); }
  void emplace_back(T&& val) { vec.emplace_back(std::move(val)); }
};

template <typename T>
struct Parser {
  static std::unique_ptr<TypedVecBase> parse(
      const std::shared_ptr<arrow::ChunkedArray>& array) {
    auto array_type = array->type();
    auto arrow_type = TypeConverter<T>::ArrowTypeValue();
    CHECK(array_type->Equals(arrow_type))
        << "Inconsistent data type, expect " << arrow_type->ToString()
        << ", but got " << array_type->ToString();
    auto ret = std::make_unique<TypedVec<T>>();
    size_t row_num = array->length();
    ret->reserve(row_num);
    for (int idx = 0; idx < array->num_chunks(); ++idx) {
      auto casted =
          std::static_pointer_cast<typename TypeConverter<T>::ArrowArrayType>(
              array->chunk(idx));
      for (int i = 0; i < casted->length(); ++i) {
        ret->emplace_back(casted->Value(i));
      }
    }
    return ret;
  }
};

template <>
struct Parser<std::string_view> {
  static std::unique_ptr<TypedVecBase> parse(
      const std::shared_ptr<arrow::ChunkedArray>& array) {
    auto type = array->type();
    CHECK(type->Equals(arrow::utf8()) || type->Equals(arrow::large_utf8()))
        << "Inconsistent data type, expect utf8 or large_utf8, but got "
        << type->ToString();
    auto ret = std::make_unique<TypedVec<std::string_view>>();

    size_t row_num = array->length();
    ret->reserve(row_num);
    if (type->Equals(arrow::large_utf8())) {
      for (int idx = 0; idx < array->num_chunks(); ++idx) {
        auto casted = std::static_pointer_cast<arrow::LargeStringArray>(
            array->chunk(idx));
        for (int64_t i = 0; i < casted->length(); ++i) {
          if (casted->IsNull(i)) {
            ret->emplace_back(std::string_view());
          } else {
            auto str = casted->GetView(i);
            ret->emplace_back(std::string_view(str.data(), str.size()));
          }
        }
      }
    } else {
      for (int idx = 0; idx < array->num_chunks(); ++idx) {
        auto casted =
            std::static_pointer_cast<arrow::StringArray>(array->chunk(idx));
        for (int64_t i = 0; i < casted->length(); ++i) {
          if (casted->IsNull(i)) {
            ret->emplace_back(std::string_view());
          } else {
            auto str = casted->GetView(i);
            ret->emplace_back(std::string_view(str.data(), str.size()));
          }
        }
      }
    }
    return ret;
  }
};

class HttpClientPool {
 public:
  HttpClientPool(const std::string& host, int port, size_t pool_size)
      : host_(host), port_(port), pool_size_(pool_size) {
    for (size_t i = 0; i < pool_size_; ++i) {
      pool_.emplace(std::make_shared<httplib::Client>(host_, port_));
    }
  }

  std::shared_ptr<httplib::Client> acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return !pool_.empty(); });

    auto client = pool_.front();
    pool_.pop();
    return client;
  }

  void release(std::shared_ptr<httplib::Client> client) {
    std::lock_guard<std::mutex> lock(mutex_);
    pool_.push(client);
    cond_.notify_one();
  }

  bool send_post_request(const std::string& path, const std::string& body) {
    auto client = acquire();
    std::string encoder = body;
    encoder.push_back(static_cast<char>(0));
    auto res = client->Post(path.c_str(), encoder.data(), encoder.size(),
                            "text/plain");
    bool ret = false;
    if (res) {
      LOG(INFO) << "Response status: " << res->status;
      if (res->status == 200) {
        ret = true;
      } else {
        LOG(ERROR) << "Response body: " << res->body;
      }
    } else {
      LOG(ERROR) << "Request failed";
    }
    release(client);
    return ret;
  }

 private:
  std::string host_;
  int port_;
  size_t pool_size_;
  std::queue<std::shared_ptr<httplib::Client>> pool_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

class IncrementalLoader {
 public:
  IncrementalLoader(gs::Schema& schema, const LoadingConfig& loading_config)
      : schema_(schema),
        loading_config_(loading_config),
        url_("127.0.0.1"),
        port_(10000),
        impl_(schema, loading_config, loading_config.GetParallelism()),
        client_pool_(url_, port_, 16) {}

  std::unique_ptr<TypedVecBase> parse_properties(
      const std::shared_ptr<arrow::ChunkedArray>& array, PropertyType type) {
    if (type == PropertyType::kInt64) {
      return Parser<int64_t>::parse(array);
    } else if (type == PropertyType::kStringView) {
      return Parser<std::string_view>::parse(array);
    } else if (type == PropertyType::kInt32) {
      return Parser<int32_t>::parse(array);
    } else if (type == PropertyType::kUInt32) {
      return Parser<uint32_t>::parse(array);
    } else if (type == PropertyType::kUInt64) {
      return Parser<uint64_t>::parse(array);
    } else if (type == PropertyType::kDate) {
      return Parser<int64_t>::parse(array);
    } else if (type == PropertyType::kDouble) {
      return Parser<double>::parse(array);
    } else if (type == PropertyType::kBool) {
      return Parser<bool>::parse(array);
    } else if (type == PropertyType::kFloat) {
      return Parser<float>::parse(array);
    } else if (type == PropertyType::kUInt32) {
      return Parser<uint32_t>::parse(array);
    } else if (type == PropertyType::kUInt64) {
      return Parser<uint64_t>::parse(array);
    } else {
      LOG(FATAL) << "Unsupported property type";
    }
  }

  void addVertexRecordBatch(
      label_t v_label_id, const std::vector<std::string>& v_files,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, const std::string&, const LoadingConfig&, int)>
          supplier_creator) {
    auto primary_keys = schema_.get_vertex_primary_key(v_label_id);
    if (primary_keys.size() != 1) {
      LOG(FATAL) << "We currently only support one primary key";
    }
    auto type = std::get<0>(primary_keys[0]);
    if (type != PropertyType::kInt64 && type != PropertyType::kStringView &&
        type != PropertyType::kInt32 && type != PropertyType::kUInt32 &&
        type != PropertyType::kUInt64) {
      LOG(FATAL)
          << "Only support int64_t, uint64_t, int32_t, uint32_t and string "
             "primary key for vertex.";
    }
    grape::BlockingQueue<std::shared_ptr<arrow::RecordBatch>> queue;
    queue.SetLimit(1024);
    std::vector<std::thread> work_threads;
    for (auto& v_file : v_files) {
      auto record_batch_supplier_vec =
          supplier_creator(v_label_id, v_file, loading_config_,
                           std::thread::hardware_concurrency());
      queue.SetProducerNum(record_batch_supplier_vec.size());
      for (size_t idx = 0; idx < record_batch_supplier_vec.size(); ++idx) {
        work_threads.emplace_back(
            [&](int i) {
              auto& record_batch_supplier = record_batch_supplier_vec[i];
              bool first_batch = true;
              while (true) {
                auto batch = record_batch_supplier->GetNextBatch();
                if (!batch) {
                  queue.DecProducerNum();
                  break;
                }
                if (first_batch) {
                  auto header = batch->schema()->field_names();
                  auto schema_column_names =
                      schema_.get_vertex_property_names(v_label_id);
                  CHECK(schema_column_names.size() + 1 == header.size())
                      << "File header of size: " << header.size()
                      << " does not match schema column size: "
                      << schema_column_names.size() + 1;
                  first_batch = false;
                }
                queue.Put(batch);
              }
            },
            idx);
      }

      for (unsigned idx = 0;
           idx < std::min(std::thread::hardware_concurrency(),
                          static_cast<unsigned>(
                              record_batch_supplier_vec.size() * 8));
           ++idx) {
        work_threads.emplace_back(
            [&](int i) {
              while (true) {
                std::shared_ptr<arrow::RecordBatch> batch{nullptr};
                auto ret = queue.Get(batch);
                if (!ret) {
                  break;
                }
                if (!batch) {
                  LOG(FATAL) << "get nullptr batch";
                }
                auto columns = batch->columns();
                std::vector<std::unique_ptr<TypedVecBase>> vecs;
                std::vector<PropertyType> types;
                auto& primary_keys = schema_.get_vertex_primary_key(v_label_id);
                for (auto& p : primary_keys) {
                  types.push_back(std::get<0>(p));
                }
                auto props = schema_.get_vertex_properties(v_label_id);
                for (auto& type : props) {
                  types.emplace_back(type);
                }
                CHECK(columns.size() == types.size());
                for (size_t i = 0; i < columns.size(); ++i) {
                  auto chunked_array =
                      std::make_shared<arrow::ChunkedArray>(columns[i]);
                  vecs.emplace_back(parse_properties(chunked_array, types[i]));
                }
                size_t row_num = vecs[0]->size();
                for (size_t i = 1; i < vecs.size(); ++i) {
                  CHECK(vecs[i]->size() == row_num);
                }
                std::vector<char> buf;
                grape::InArchive arc;
                for (size_t i = 0; i < row_num; ++i) {
                  arc << static_cast<uint8_t>(0);
                  arc << static_cast<uint8_t>(v_label_id);
                  for (size_t j = 0; j < vecs.size(); ++j) {
                    vecs[j]->serialize(arc, i);
                  }
                }
                std::string ss(arc.GetBuffer(), arc.GetSize());
                ss.push_back(
                    static_cast<char>(gs::Schema::BATCH_UPDATE_PLUGIN_ID));
                if (client_pool_.send_post_request("/v1/graph/current/query",
                                                   ss)) {
                  LOG(INFO) << "add " << row_num << " vertices success";
                } else {
                  LOG(ERROR) << "add " << row_num << " vertices failed";
                }
              }
            },
            idx);
      }

      for (auto& t : work_threads) {
        t.join();
      }
      work_threads.clear();
    }
  }

  void AddEdgesRecordBatch(
      label_t src_label_id, label_t dst_label_id, label_t e_label_id,
      const std::vector<std::string>& filenames,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&,
          int)>
          supplier_creator) {
    grape::BlockingQueue<std::shared_ptr<arrow::RecordBatch>> queue;
    queue.SetLimit(1024);
    std::vector<std::thread> work_threads;
    for (auto& e_file : filenames) {
      auto record_batch_supplier_vec = supplier_creator(
          src_label_id, dst_label_id, e_label_id, e_file, loading_config_,
          std::thread::hardware_concurrency());
      queue.SetProducerNum(record_batch_supplier_vec.size());
      for (size_t i = 0; i < record_batch_supplier_vec.size(); ++i) {
        work_threads.emplace_back(
            [&](int idx) {
              auto& record_batch_supplier = record_batch_supplier_vec[idx];
              bool first_batch = true;
              while (true) {
                auto record_batch = record_batch_supplier->GetNextBatch();
                if (!record_batch) {
                  queue.DecProducerNum();
                  break;
                }
                if (first_batch) {
                  auto header = record_batch->schema()->field_names();
                  auto schema_column_names = schema_.get_edge_property_names(
                      src_label_id, dst_label_id, e_label_id);
                  auto schema_column_types = schema_.get_edge_properties(
                      src_label_id, dst_label_id, e_label_id);
                  CHECK(schema_column_names.size() + 2 == header.size())
                      << "schema size: " << schema_column_names.size()
                      << " neq header size: " << header.size();
                  first_batch = false;
                }

                queue.Put(record_batch);
              }
            },
            i);
      }

      for (size_t i = 0;
           i <
           std::min(static_cast<unsigned>(8 * record_batch_supplier_vec.size()),
                    std::thread::hardware_concurrency());
           ++i) {
        work_threads.emplace_back(
            [&](int idx) {
              while (true) {
                std::shared_ptr<arrow::RecordBatch> record_batch{nullptr};
                auto ret = queue.Get(record_batch);
                if (!ret) {
                  break;
                }
                if (!record_batch) {
                  LOG(FATAL) << "get nullptr batch";
                }
                auto columns = record_batch->columns();
                std::vector<std::unique_ptr<TypedVecBase>> vecs;
                std::vector<PropertyType> types;
                auto& src_type =
                    schema_.get_vertex_primary_key(src_label_id)[0];
                types.emplace_back(std::get<0>(src_type));
                auto& dst_type =
                    schema_.get_vertex_primary_key(dst_label_id)[0];
                types.emplace_back(std::get<0>(dst_type));
                auto props = schema_.get_edge_properties(
                    src_label_id, dst_label_id, e_label_id);
                for (auto& type : props) {
                  types.emplace_back(type);
                }
                CHECK(columns.size() == types.size());

                for (size_t i = 0; i < columns.size(); ++i) {
                  auto chunked_array =
                      std::make_shared<arrow::ChunkedArray>(columns[i]);
                  vecs.emplace_back(parse_properties(chunked_array, types[i]));
                }
                size_t row_num = vecs[0]->size();
                for (size_t i = 1; i < vecs.size(); ++i) {
                  CHECK(vecs[i]->size() == row_num);
                }
                std::vector<char> buf;
                grape::InArchive arc;

                for (size_t i = 0; i < row_num; ++i) {
                  arc << static_cast<uint8_t>(1);
                  arc << static_cast<uint8_t>(src_label_id);
                  vecs[0]->serialize(arc, i);
                  arc << static_cast<uint8_t>(dst_label_id);
                  vecs[1]->serialize(arc, i);
                  arc << static_cast<uint8_t>(e_label_id);
                  if (vecs.size() > 3) {
                    arc << vecs.size() - 2;
                  }
                  for (size_t j = 2; j < vecs.size(); ++j) {
                    vecs[j]->serialize(arc, i);
                  }
                }

                std::string ss(arc.GetBuffer(), arc.GetSize());
                ss.push_back(
                    static_cast<char>(gs::Schema::BATCH_UPDATE_PLUGIN_ID));
                if (client_pool_.send_post_request("/v1/graph/current/query",
                                                   ss)) {
                  LOG(INFO) << "add " << row_num << " edges success";
                } else {
                  LOG(ERROR) << "add " << row_num << " edges failed";
                }
              }
            },
            i);
      }
      for (auto& t : work_threads) {
        t.join();
      }
    }
  }

  void loadVertices() {
    impl_.loadVertices(
        [this](label_t label, const std::vector<std::string>& files,
               std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
                   label_t, const std::string&, const LoadingConfig&, int)>
                   supplier_creator) {
          addVertexRecordBatch(label, files, supplier_creator);
        });
  }

  void loadEdges() {
    impl_.loadEdges(
        [this](label_t src_label_id, label_t dst_label_id, label_t e_label_id,
               const std::vector<std::string>& filenames,
               std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
                   label_t, label_t, label_t, const std::string&,
                   const LoadingConfig&, int)>
                   supplier_creator) {
          AddEdgesRecordBatch(src_label_id, dst_label_id, e_label_id, filenames,
                              supplier_creator);
        });
  }

  Result<bool> LoadFragment() {
    try {
      loadVertices();
      loadEdges();
      return Result<bool>(true);
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to load fragment: " << e.what();
      return Result<bool>(StatusCode::INTERNAL_ERROR,
                          "Load fragment failed: " + std::string(e.what()),
                          false);
    }
  }

 private:
  const Schema& schema_;
  const LoadingConfig& loading_config_;

  std::string url_;
  int port_;

  ODPSFragmentLoaderImpl impl_;
  HttpClientPool client_pool_;
};

}  // namespace gs

namespace bpo = boost::program_options;
int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("parallelism,p", bpo::value<uint32_t>(),
                                      "parallelism of loader")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file");
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }

  std::string bulk_load_config_path = "";
  std::string graph_schema_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();

  if (!vm.count("bulk-load")) {
    LOG(ERROR) << "bulk-load-config is required";
    return -1;
  }
  bulk_load_config_path = vm["bulk-load"].as<std::string>();

  auto schema_res = gs::Schema::LoadFromYaml(graph_schema_path);
  auto loading_config = gs::LoadingConfig::ParseFromYamlFile(
      schema_res.value(), bulk_load_config_path);
  if (vm.count("parallelism")) {
    loading_config.value().SetParallelism(vm["parallelism"].as<uint32_t>());
  }
  gs::IncrementalLoader loader(schema_res.value(), loading_config.value());
  loader.LoadFragment();
  return 0;
}
