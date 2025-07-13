#ifndef CHUNKED_RUNTIME_UTILS_CONFIGS_H_
#define CHUNKED_RUNTIME_UTILS_CONFIGS_H_
#include <cstddef>

namespace gs {
namespace chunked_runtime {
struct Configs {
  constexpr static size_t CHUNK_SIZE = 4096;  // 2KB

  constexpr static int MAX_THREAD_NUM =
      1;  // Maximum number of threads for parallel execution
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_CONFIGS_H_