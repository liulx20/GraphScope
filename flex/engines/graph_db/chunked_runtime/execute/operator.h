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

#ifndef CHUNKED_RUNTIME_EXECUTE_OPERATOR_H_
#define CHUNKED_RUNTIME_EXECUTE_OPERATOR_H_

#include <map>

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/engines/graph_db/runtime/utils/opr_timer.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/engines/graph_db/chunked_runtime/common/datachunks/datachunks.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace chunked_runtime {

using gs::runtime::GraphReadInterface;
class IOprState {
 public:
  virtual ~IOprState() = default;
  virtual void clear() = 0;
  virtual bool getNextChunks(DataChunks& chunks) = 0;
  virtual std::shared_ptr<IOprState> src_state() = 0;
  virtual bool initialized() const { return false; }
  virtual DataChunks& src_chunks() = 0;
};

class IReadOpr {
 public:
  virtual ~IReadOpr() = default;

  virtual std::string get_operator_name() const = 0;

  virtual bl::result<void> EvalChunks(const GraphReadInterface&,
                                      const std::map<std::string, std::string>&,
                                      IOprState&) = 0;
  virtual IReadOpr* source_opr() const {
    return nullptr;  // Default implementation returns nullptr
  }

  virtual std::shared_ptr<IOprState> initState(
      std::shared_ptr<IOprState> state_from_other_pipeline) = 0;
  bl::result<bool> getNextChunks(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, IOprState& state,
      DataChunks& chunks) {
    if (!state.initialized() || !state.getNextChunks(chunks)) {
      if (source_opr() != nullptr) {
        state.clear();
        if (source_opr()
                ->getNextChunks(graph, params, *state.src_state(),
                                state.src_chunks())
                .value()) {
          EvalChunks(graph, params, state);
          return state.getNextChunks(chunks);
        }
        return false;
      } else {
        EvalChunks(graph, params, state);
        return state.getNextChunks(chunks);
      }
    }
    return true;
  }
  /*virtual bl::result<Context> Eval(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer& timer) = 0;*/
};

class IReadSinkOpr : public IReadOpr {
 public:
  virtual ~IReadSinkOpr() = default;

  virtual void execute(const GraphReadInterface& graph,
                       const std::map<std::string, std::string>& params,
                       IOprState& state) = 0;
};
using gs::runtime::ContextMeta;
using ReadOpBuildResultT =
    std::pair<std::unique_ptr<IReadOpr>, gs::runtime::ContextMeta>;
class IReadOperatorBuilder {
 public:
  virtual ~IReadOperatorBuilder() = default;
  virtual bl::result<ReadOpBuildResultT> Build(
      std::unique_ptr<IReadOpr>& source_opr, const gs::Schema& schema,
      const ContextMeta& ctx_meta, const physical::PhysicalPlan& plan,
      int op_idx) = 0;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

template <typename FUNC>
bl::result<void> ForEachChunk(DataChunks& datachunks, FUNC&& func) {
  size_t chunk_num = datachunks.chunk_num();
  for (size_t i = 0; i < chunk_num; ++i) {
    func(datachunks[i]);
  }
  return bl::result<void>();
}

}  // namespace chunked_runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPERATOR_H_