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
#ifndef ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
#define ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_

#include <vector>
#include "flex/third_party/httplib.h"
#include "flex/utils/app_utils.h"
#include "flex/utils/property/type.h"

namespace server {
class GraphDBUpdateServer {
 public:
  static GraphDBUpdateServer& get() {
    static GraphDBUpdateServer db_server;
    return db_server;
  }

  static void init_client(httplib::Client*& cli, const std::string& url,
                          int port) {
    if (cli != nullptr) {
      delete cli;
    }
    cli = new httplib::Client(url, port);
    cli->set_connection_timeout(0, 300000);
    cli->set_read_timeout(60, 0);
    cli->set_write_timeout(60, 0);
  }

  static void init(const std::string& url, int port) {
    auto& server = GraphDBUpdateServer::get();
    server.url_ = url;
    server.port_ = port;
    init_client(server.cli_, url, port);
  }

  ~GraphDBUpdateServer() {
    if (cli_ != nullptr) {
      delete cli_;
    }
  }

  auto parse_prop_map(gs::Decoder& output) {
    int len = output.get_int();
    std::unordered_map<std::string_view, std::string_view> prop_map;
    for (int i = 0; i < len; ++i) {
      const auto& key = output.get_string();
      const auto& value = output.get_string();
      prop_map[key] = value;
    }
    return prop_map;
  }

  template <typename T>
  void get_numerical_val(const std::string_view& str, T& val) {
    val = 0;
    for (char c : str) {
      val = val * 10 + c - '0';
    }
  }
  gs::Any parse_prop_value(const std::string_view& prop_str,
                           const gs::PropertyType& type) {
    if (type == gs::PropertyType::kString) {
      return gs::Any::From(prop_str);
    } else if (type == gs::PropertyType::kInt64) {
      int64_t val;
      get_numerical_val(prop_str, val);
      return gs::Any::From(val);
    } else if (type == gs::PropertyType::kInt32) {
      int32_t val;
      get_numerical_val(prop_str, val);
      return gs::Any::From(val);
    } else if (type == gs::PropertyType::kDate) {
      int64_t val;
      get_numerical_val(prop_str, val);
      return gs::Any::From(gs::Date(val));
    } else if (type == gs::PropertyType::kDouble) {
      auto prop = std::string(prop_str.data(), prop_str.size());
      double val = std::atof(prop.c_str());
      return gs::Any::From(val);
    } else if (type == gs::PropertyType::kEmpty) {
      return gs::Any();
    }
    return gs::Any();
  }

  size_t get_any_size(const Any& any) {
    if (any.type == gs::PropertyType::kString) {
      return 4 + any.s.size();
    } else if (any.type == gs::PropertyType::kDate) {
      return 8;
    } else if (any.type == gs::PropertyType::kInt64) {
      return 8;
    } else if (any.type == gs::PropertyType::kDouble) {
      return 8;
    } else if (any.type == gs::PropertyType::kInt32) {
      return 4;
    } else if (any.type == gs::PropertyType::kEmpty) {
      return 0;
    }
  }
  size_t parse_vertex(
      gs::Decoder& output,
      std::tuple<std::string, gs::Any, std::vector<gs::Any>>& vertex) {
    size_t len = 0;
    const auto& label = output.get_string();
    len += label.size() + 4;
    std::string label_name = std::string(label.data(), label.size());
    auto label_id = schema_.get_vertex_label_id(label_name);
    const auto& prop_names = schema_.get_vertex_property_names(label_id);
    const auto& prop_types = schema_.get_vertex_properties(label_id);
    const auto& vertex_prop = parse_prop_map(output);
    const auto& prop_names = schema_.get_vertex_property_names(label_id);
    const auto& prop_types = schema_.get_vertex_properties(label_id);
    const auto& prim_keys = schema_.get_vertex_primary_key(label_id);
    std::get<0>(vertex) = label_name;
    for (const auto& [type, name, _] : prim_keys) {
      std::get<1>(vertex) = parse_prop_value(vertex_prop.at(name), type);
      len += get_any_size(std::get<1>(vertex));
      break;
    }
    auto& props = std::get<2>(vertex);
    for (size_t i = 0; i < prop_names.size(); ++i) {
      const auto& name = prop_names[i];
      const auto& type = prop_types[i];
      auto prop_name = vertex_prop.at(name);
      auto val = parse_prop_value(prop_name, type);
      len += get_any_size(val);
      props.emplace_back(val);
    }
    return len;
  }

  size_t parse_edge(gs::Decoder& output,
                    std::tuple<std::string, gs::Any, std::string, gs::Any,
                               std::string, gs::Any>& edge) {
    const auto& src_label = output.get_string();
    size_t len = src_label.size() + 4;
    std::string src_label_str = std::string(src_label.data(), src_label.size());
    std::get<0>(edge) = src_label_str;
    const auto& dst_label = output.get_string();
    std::string dst_label_str = std::string(dst_label.data(), dst_label.size());
    std::get<2>(edge) = dst_label_str;
    len += dst_label.size() + 4;
    const auto& edge_label = output.get_string();
    len += edge_label.size() + 4;
    std::string edge_label_str =
        std::string(edge_label.data(), edge_label.size());
    std::get<4>(edge) = edge_label_str;
    label_t src_label_id = schema_.get_vertex_label_id(src_label_str);
    const auto& src_pk = parse_prop_map(output);
    const auto& src_prim_keys = schema_.get_vertex_primary_key(src_label_id);
    for (const auto& [type, name, _] : src_prim_keys) {
      std::get<1>(edge) = parse_prop_value(src_pk.at(name), type);
      len += get_any_size(std::get<1>(edge));
      break;
    }
    label_t dst_label_id = schema_.get_vertex_label_id(dst_label_str);
    const auto& dst_pk = parse_prop_map(output);
    const auto& dst_prim_keys = schema_.get_vertex_primary_key(dst_label_id);
    for (const auto& [type, name, _] : dst_prim_keys) {
      std::get<3>(edge) = parse_prop_value(dst_pk.at(name), type);
      len += get_any_size(std::get<3>(edge));
      break;
    }
    label_t edge_label_id = schema_.get_edge_label_id(edge_label_str);
    const auto& prop = parse_prop_map(output);
    const auto& edge_prop =
        schema_.get_edge_properties(src_label_id, dst_label_id, edge_label_id);
    const auto& edge_prop_name =
        get_edge_property_names(src_label_id, dst_label_id, edge_label_id);
    for (size_t i = 0; i < edge_prop.size(); ++i) {
      const auto& name = edge_prop_name[i];
      const auto& type = edge_prop[i];
      std::get<5>(edge) = parse_prop_value(edge_prop.at(name), type);
      len += get_any_size(std::get<5>(edge));
      break;
    }
    return len;
  }
  void parse_query(const std::string& origin_query, std::string& query) {
    gs::Decoder output(origin_query.data(), origin_query.size());
    size_t len = 4;
    int vertex_len = output.get_int();
    std::vector<std::tuple<std::string, gs::Any, std::vector<gs::Any>>>
        vertices;
    for (int i = 0; i < vertex_len; ++i) {
      std::tuple<std::string, gs::Any, std::vector<gs::Any>> vertex;
      len += parse_vertex(output, vertex);
      vertices.emplace_back(vertex);
    }
    std::vector<std::tuple<std::string, gs::Any, std::string, gs::Any,
                           std::string, gs::Any>>
        edges;
    len += 4;
    int edge_len = output.get_int();
    for (int i = 0; i < edge_len; ++i) {
      std::tuple<std::string_view, gs::Any, std::string_view, gs::Any,
                 std::string_view, gs::Any>
          edge;
      len += parse_edge(output, edge);
      edges.emplace_back(edge);
    }
    std::vector<char> data(len + 1);
    gs::Encoder input(data);
    input.put_int(vertices.size());
    for (auto& [label, id, props] : vertices) {
      input.put_string(label);
    }
    input.put_int(edges.size());
    for (auto& [src_label, src_id, dst_label, dst_id, edge_label, edge_prop] :
         edges) {
      input.put_string(src_label);
      input.put_string(dst_label);
      input.put_string(edge_label);
    }
    input.put_byte(3);
    query = std::string(data.data(), data.size());
  }
  void Eval(const std::string& origin_query) {
    std::string query{};
    parse_query(origin_query, query);
    printf("query: %s\n", query.c_str());

    size_t len = query.size();
    if (size_ + sizeof(size_t) + len >= update_queries_.capacity()) {
      Forward();
    }
    while (sizeof(size_t) + len >= update_queries_.capacity()) {
      update_queries_.reserve(2 * update_queries_.capacity());
    }
    memcpy(update_queries_.data() + size_, &len, sizeof(size_t));
    size_ += sizeof(size_t);
    memcpy(update_queries_.data() + size_, query.data(), len);
    size_ += len;
    static constexpr uint8_t flag = (1 << 7);
    if (static_cast<uint8_t>(query.back()) >= flag) {
      update_queries_[size_ - 1] -= flag;
      Forward();
    }
  }

  void Forward() {
    printf("Post %ld\n", size_);
    if (size_ <= 0) {
      return;
    }
    const auto& result = cli_->Post(
        "/interactive/update", update_queries_.data(), size_, "text/plain");
    printf("code: %d\n", result->status);

    size_ = 0;
  }

  void init(gs::Schema& schema) { schema_ = schema; }

 private:
  std::vector<char> update_queries_;
  size_t size_;
  std::string url_;
  int port_;
  httplib::Client* cli_;
  gs::Schema schema_;
  GraphDBUpdateServer()
      : size_(0), url_("127.0.0.1"), port_(10000), cli_(nullptr) {
    update_queries_.reserve(4096);
    init_client(cli_, url_, port_);
  };
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
