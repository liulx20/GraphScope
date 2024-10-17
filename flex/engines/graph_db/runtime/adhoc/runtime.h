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
#ifndef RUNTIME_ADHOC_RUNTIME_H_
#define RUNTIME_ADHOC_RUNTIME_H_

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

namespace runtime {

class OpCost {
 public:
  OpCost() {}
  ~OpCost() {
    LOG(INFO) << "op elapsed time: ";
    for (auto& pair : table) {
      LOG(INFO) << "\t" << pair.first << ": " << pair.second << " ("
                << pair.second / total * 100.0 << "%)";
    }
  }

  static OpCost& get() {
    static OpCost instance;
    return instance;
  }

  void add_total(double t) { total += t; }

  void output(const std::string& path) const {
    std::ofstream fout(path);
    for (auto& pair : table) {
      fout << pair.first << ": " << pair.second << " ("
           << pair.second / total * 100.0 << "%)" << std::endl;
    }
    fout.flush();
  }

  void clear() {
    total = 0;
    table.clear();
  }

  std::map<std::string, double> table;
  double total = 0;
};

Context runtime_eval(const physical::PhysicalPlan& plan,
                     const ReadTransaction& txn,
                     const std::map<std::string, std::string>& params);
WriteContext runtime_eval(const physical::PhysicalPlan& plan,
                          InsertTransaction& txn,
                          const std::map<std::string, std::string>& params);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_H_