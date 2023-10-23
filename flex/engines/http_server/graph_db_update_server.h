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

#include "flex/third_party/httplib.h"
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
  void Eval(const std::string& query) {
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

 private:
  std::vector<char> update_queries_;
  size_t size_;
  std::string url_;
  int port_;
  httplib::Client* cli_;
  GraphDBUpdateServer()
      : size_(0), url_("127.0.0.1"), port_(10000), cli_(nullptr) {
    update_queries_.reserve(4096);
    init_client(cli_, url_, port_);
  };
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
