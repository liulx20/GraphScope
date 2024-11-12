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

#ifndef ENGINES_GRAPH_DB_BATCH_UPDATE_APP_H_
#define ENGINES_GRAPH_DB_BATCH_UPDATE_APP_H_
#include "flex/engines/graph_db/app/app_base.h"

namespace gs {
class BatchUpdateApp : public WriteAppBase {
 public:
  BatchUpdateApp(const GraphDB& db) : db_(db) {}

  AppType type() const override { return AppType::kBuiltIn; }

  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) override;

 private:
  const GraphDB& db_;
};

class BatchUpdateAppFactory : public AppFactoryBase {
 public:
  BatchUpdateAppFactory() = default;
  ~BatchUpdateAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs
#endif  // ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_