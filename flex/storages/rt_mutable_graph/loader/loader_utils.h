
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
#include "flex/utils/property/column.h"

#include <condition_variable>
#include <mutex>
#include <queue>
namespace gs {

// A simple queue which stores the record batches, for consuming.
template <typename EDATA_T>
struct ConsumerQueue {
 public:
  ConsumerQueue(int32_t max_length = 2048)
      : max_length_(max_length), finished_(false) {}

  void push(const EDATA_T& data) {
    std::unique_lock<std::mutex> lock(mutex_);
    VLOG(10) << "Try Pushing item to queue, size: " << queue_.size()
             << ", max_length_: " << max_length_;
    full_cv_.wait(lock,
                  [this] { return queue_.size() < max_length_ || finished_; });
    if (finished_) {
      return;
    }
    VLOG(10) << "Pushing item to queue, size: " << queue_.size()
             << ", max_length_: " << max_length_;
    queue_.push(data);
    empty_cv_.notify_one();
  }

  EDATA_T pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    VLOG(10) << "Try Popping item from queue, size: " << queue_.size()
             << ", max_length_: " << max_length_;
    empty_cv_.wait(lock, [this] { return !queue_.empty() || finished_; });
    if (queue_.empty()) {
      return nullptr;
    }
    VLOG(10) << "Popping item from queue, size: " << queue_.size()
             << ", max_length_: " << max_length_;
    auto data = queue_.front();
    queue_.pop();
    full_cv_.notify_one();
    return data;
  }

  size_t size() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.size();
  }

  void finish() {
    std::unique_lock<std::mutex> lock(mutex_);
    finished_ = true;
    empty_cv_.notify_all();
    full_cv_.notify_all();
  }

 private:
  std::queue<EDATA_T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable full_cv_;
  std::condition_variable empty_cv_;
  size_t max_length_;
  bool finished_;
};

template <typename EDATA_T>
class MMapVector {
 public:
  MMapVector(const std::string& work_dir, const std::string& file_name)
      : work_dir_(work_dir), file_name_(file_name) {
    array_.open(work_dir + "/" + file_name_ + ".tmp", false);
    array_.resize(4096);
    size_ = 0;
    cap_ = 4096;
  }

  ~MMapVector() {
    array_.reset();
    unlink((work_dir_ + "/" + file_name_ + ".tmp").c_str());
  }

  MMapVector(MMapVector&& other) {
    array_.swap(other.array_);
    size_ = other.size_;
    cap_ = other.cap_;
    file_name_.swap(other.file_name_);
    work_dir_.swap(other.work_dir_);
  }

  void push_back(const EDATA_T& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void emplace_back(EDATA_T&& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void resize(size_t size) {
    while (size > cap_) {
      cap_ *= 2;
      array_.resize(cap_);
    }
    size_ = size;
  }

  size_t size() const { return size_; }

  const EDATA_T& operator[](size_t index) const { return array_[index]; }
  EDATA_T& operator[](size_t index) { return array_[index]; }
  const EDATA_T* begin() const { return array_.data(); }
  const EDATA_T* end() const { return array_.data() + size_; }

  void clear() {
    size_ = 0;
    cap_ = 0;
  }

 private:
  mmap_array<EDATA_T> array_;
  std::string work_dir_;
  std::string file_name_;
  size_t size_;
  size_t cap_;
};
};      // namespace gs
#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
