#include "flex/engines/graph_db/runtime/execute/ops/retrieve/lambda_wrapper.h"

namespace gs {
namespace runtime {
namespace ops {
void* LambdaExecutor(void* args) {
  auto* wrapper = static_cast<LambdaWrapperBase*>(args);
  wrapper->func();
  return nullptr;
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs