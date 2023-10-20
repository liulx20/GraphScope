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

#include <seastar/core/print.hh>

namespace server {
class GraphDBUpdateServer {
 public:
  static GraphDBUpdateServer get() {
    static GraphDBUpdateServer db_server;
    return db_server;
  }
  void Eval(const std::string& query) {
    printf("query: %s\n", query.c_str());
    update_queries_.emplace_back(query);
  }

 private:
  std::vector<std::string> update_queries_;
  ;
  GraphDBUpdateServer() = default;
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
