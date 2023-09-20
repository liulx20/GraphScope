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

#ifndef GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
#define GRAPHSCOPE_UTILS_MMAP_ARRAY_H_

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>

#include "glog/logging.h"

namespace gs {

template <typename T>
class mmap_array {
 public:
  mmap_array() : fd_(-1), data_(NULL), size_(0) {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    if (data_ != NULL) {
      munmap(data_, size_);
      data_ = NULL;
    }
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
  }

  void open(const std::string& filename) {
    reset();
    fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT);
    size_t file_size = std::filesystem::file_size(filename);
    size_ = file_size / sizeof(T);
    if (size_ == 0) {
      data_ = NULL;
    } else {
      data_ = reinterpret_cast<T*>(mmap(
          NULL, size_ * sizeof(T), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
      assert(data_ != MAP_FAILED);
    }
  }

  void resize(size_t size) {
    assert(fd_ != -1);

    if (size == size_) {
      return;
    }

    if (data_ != NULL) {
      munmap(data_, size_ * sizeof(T));
    }
    ftruncate(fd_, size * sizeof(T));
    if (size == 0) {
      data_ = NULL;
    } else {
      data_ = static_cast<T*>(::mmap(
          NULL, size * sizeof(T), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    }
    size_ = size;
  }

  T* data() { return data_; }
  const T* data() const { return data_; }

  void set(size_t idx, const T& val) { data_[idx] = val; }

  const T& get(size_t idx) const { return data_[idx]; }

  const T& operator[](size_t idx) const { return data_[idx]; }
  T& operator[](size_t idx) { return data_[idx]; }

  size_t size() const { return size_; }

  void swap(mmap_array<T>& rhs) {
    std::swap(fd_, rhs.fd_);
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
  }

 private:
  int fd_;
  T* data_;
  size_t size_;
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    items_.reset();
    data_.reset();
  }

  void open(const std::string& filename) {
    items_.open(filename + ".items");
    data_.open(filename + ".data");
  }

  void resize(size_t size, size_t data_size) {
    items_.resize(size);
    data_.resize(data_size);
  }

  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    memcpy(data_.data() + offset, val.data(), val.size());
  }

  std::string_view get(size_t idx) const {
    const string_item& item = items_.get(idx);
    return std::string_view(data_.data() + item.offset, item.length);
  }

  size_t size() const { return items_.size(); }

  size_t data_size() const { return data_.size(); }

  void swap(mmap_array& rhs) {
    items_.swap(rhs.items_);
    data_.swap(rhs.data_);
  }

 private:
  mmap_array<string_item> items_;
  mmap_array<char> data_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
