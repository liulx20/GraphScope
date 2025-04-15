#include <brpc/server.h>
#include <bthread/bthread.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include "flex/bin/generated/interactives.pb.h"
#define BTHREAD 1
class QueryServiceImpl : public interactives::QueryService {
 public:
  QueryServiceImpl() {}
  ~QueryServiceImpl() override {}
  void CypherQuery(::google::protobuf::RpcController* cntl_base,
                   const interactives::QueryRequest* request,
                   interactives::QueryResponse* response,
                   ::google::protobuf::Closure* done) override {
    // response->set_message(request->get_message());
  }
};

// 定义矩阵为二维向量
using Matrix = std::vector<std::vector<double>>;

// 函数：初始化矩阵为随机值
Matrix initialize_matrix(int rows, int cols) {
  Matrix mat(rows, std::vector<double>(cols, 0.0));
  for (int i = 0; i < rows; ++i)
    for (int j = 0; j < cols; ++j)
      mat[i][j] = rand() % 100;  // 随机初始化为0-99的整数
  return mat;
}

// 函数：打印矩阵（仅用于小矩阵）
void print_matrix(const Matrix& mat) {
  for (const auto& row : mat) {
    for (const auto& val : row)
      std::cout << val << "\t";
    std::cout << "\n";
  }
  std::cout << "\n";
}

// 函数：计算结果矩阵的部分行
void multiply_part(const Matrix& A, const Matrix& B, Matrix& C, int start_row,
                   int end_row) {
  int cols_B = B[0].size();
  int cols_A = A[0].size();
  for (int i = start_row; i < end_row; ++i) {
    for (int j = 0; j < cols_B; ++j) {
      double sum = 0.0;
      for (int k = 0; k < cols_A; ++k)
        sum += A[i][k] * B[k][j];
      C[i][j] = sum;
    }
  }
}

void cal(int64_t start_i, int64_t step, int64_t maxi, int64_t& sum,
         std::thread::id& id) {
  auto isprime = [](int64_t n) {
    if (n <= 1)
      return false;
    for (int64_t i = 2; i * i <= n; ++i) {
      if (n % i == 0)
        return false;
    }
    return true;
  };
  int64_t i = start_i;
  id = std::this_thread::get_id();
  while (i < maxi) {
    for (int64_t j = 0; j < step; i++, j++) {
      if (isprime(i)) {
        sum += i;
      }
    }
    i += step * step;
  }
  CHECK(id == std::this_thread::get_id());
}

struct LambdaWrapperBase {
  virtual void operator()() = 0;
};
template <typename Lambda>
struct LambdaWrapper : public LambdaWrapperBase {
  Lambda lambda;
  LambdaWrapper(Lambda&& l) : lambda(std::move(l)) {}
  void operator()() override { lambda(); }
};

auto func(void* arg) -> void* {
  auto wrapper = static_cast<LambdaWrapperBase*>(arg);
  (*wrapper)();
  return nullptr;
}

int main() {
  // 矩阵大小（可调整为更大的值以测试性能）
  int rows_A = 6144;
  int cols_A = 6144;
  int rows_B = 6144;
  int cols_B = 6144;
#if BTHREAD
  bthread_setconcurrency(192);
  /**brpc::Server server;
  QueryServiceImpl query_service_impl;
  if (server.AddService(&query_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) !=
      0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }

  brpc::ServerOptions options;
  options.idle_timeout_sec = 3600;
  options.num_threads = 192;

  butil::EndPoint point;
  std::string listen_addr = "192.168.0.188:8000";
  if (butil::str2endpoint(listen_addr.c_str(), &point) < 0) {
    LOG(ERROR) << "Invalid listen address:" << listen_addr;
    return -1;
  }

  if (server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start EchoServer";
    return -1;
  }*/
#endif

  if (cols_A != rows_B) {
    std::cerr << "Matrix dimensions mismatch for multiplication.\n";
    return -1;
  }

  // 初始化矩阵 A 和 B
  Matrix A = initialize_matrix(rows_A, cols_A);
  Matrix B = initialize_matrix(rows_B, cols_B);
  Matrix C(rows_A, std::vector<double>(cols_B, 0.0));

  // 确定线程数量
  unsigned int num_threads = 192;
  std::cout << "Using " << num_threads << " threads.\n";
#if BTHREAD
  std::vector<bthread_t> threads(num_threads);
#else
  std::vector<std::thread> threads(num_threads);
#endif
  int rows_per_thread = rows_A / num_threads;
  int current_row = 0;
  int64_t max_i = 1e9;
  // 创建线程并分配任务
  std::vector<std::unique_ptr<LambdaWrapperBase>> thread_wrappers(num_threads);
  std::vector<int64_t> sum(num_threads);
  std::vector<std::thread::id> ids(num_threads);
  for (unsigned int i = 0; i < num_threads; ++i) {
    int start_row = current_row;
    int end_row = start_row + rows_per_thread;
    current_row = end_row;
    // std::cout << start_row << " " << end_row << "\n";
    /*auto eval = [i, num_threads, max_i, &sum, &ids]() {
      cal(i * num_threads, num_threads, max_i, sum[i], ids[i]);
    };*/
    auto eval = [start_row, end_row, &A, &B, &C]() {
      multiply_part(A, B, C, start_row, end_row);
    };
    thread_wrappers[i] = std::move(
        std::make_unique<LambdaWrapper<decltype(eval)>>(std::move(eval)));
  }

  auto start_time = std::chrono::high_resolution_clock::now();

  for (unsigned int i = 0; i < num_threads; ++i) {
#if BTHREAD
    if (bthread_start_background(
            &threads[i], nullptr, func,
            static_cast<void*>(thread_wrappers[i].get())) != 0) {
      std::cerr << "Failed to create thread " << i << "\n";
      return -1;
    }
#else
    threads[i] =
        std::thread(func, static_cast<void*>(thread_wrappers[i].get()));
#endif
  }

#if BTHREAD
  // 等待所有线程完成
  for (auto& th : threads) {
    bthread_join(th, nullptr);
  }
#else
  for (auto& th : threads) {
    th.join();
  }
#endif

  auto end_time = std::chrono::high_resolution_clock::now();

  // 计算并输出耗时
  std::chrono::duration<double> diff = end_time - start_time;
  std::cout << "Multithreaded multiplication completed in " << diff.count()
            << " seconds.\n";

  return 0;
}