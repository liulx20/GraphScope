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
#include "flex/engines/graph_db/chunked_runtime/execute/ops/retrieve/project.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
class ProjectOpr : public IReadOpr {
 public:
  ProjectOpr(std::unique_ptr<IReadOpr>&& src_opr)
      : source_opr_(std::move(src_opr)) {}
  std::unique_ptr<IReadOpr> source_opr_;

  std::string get_operator_name() const override { return "ProjectOpr"; }

  IReadOpr* source_opr() const override { return source_opr_.get(); }
};
}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs