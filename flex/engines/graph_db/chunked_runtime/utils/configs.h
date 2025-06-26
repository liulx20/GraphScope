#ifndef CHUNKED_RUNTIME_UTILS_CONFIGS_H_
#define CHUNKED_RUNTIME_UTILS_CONFIGS_H_
#include <cstddef>

namespace gs {
namespace chunked_runtime {
struct Configs {
  constexpr static size_t CHUNK_SIZE = 64 * 1024;  // 64K

  constexpr static int MAX_THREAD_NUM =
      16;  // Maximum number of threads for parallel execution
};
}  // namespace chunked_runtime
}  // namespace gs
#endif  // CHUNKED_RUNTIME_UTILS_CONFIGS_H_