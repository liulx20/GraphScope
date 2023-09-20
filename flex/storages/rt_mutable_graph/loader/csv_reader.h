#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_H_

#include <memory>
#include <mutex>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/util/value_parsing.h>

namespace gs {

class CSVReaderBase {
 public:
  virtual ~CSVReaderBase() = default;

  virtual std::shared_ptr<arrow::RecordBatch> Read(
      size_t limit = std::numeric_limits<size_t>::max()) = 0;
};

class CSVStreamReader : public CSVReaderBase {
 public:
  CSVStreamReader(const std::string& filename,
                  const arrow::csv::ConvertOptions& convert_options,
                  const arrow::csv::ReadOptions& read_options,
                  const arrow::csv::ParseOptions& parse_options)
      : current_batch_(nullptr),
        current_batch_offset_(0),
        current_batch_size_(0) {
    auto file = arrow::io::ReadableFile::Open(filename).ValueOrDie();
    reader_ = arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                                file, read_options,
                                                parse_options, convert_options)
                  .ValueOrDie();
  }

  std::shared_ptr<arrow::RecordBatch> Read(
      size_t limit = std::numeric_limits<size_t>::max()) override {
    std::lock_guard<std::mutex> lock(lock_);
    while (current_batch_ == nullptr ||
           current_batch_offset_ == current_batch_size_) {
      auto status = reader_->ReadNext(&current_batch_);
      CHECK(status.ok());
      if (current_batch_ == nullptr) {
        return nullptr;
      }
      current_batch_offset_ = 0;
      current_batch_size_ = current_batch_->num_rows();
    }
    auto res = current_batch_->Slice(
        current_batch_offset_,
        std::min(limit, current_batch_size_ - current_batch_offset_));
    current_batch_offset_ += res->num_rows();
    return res;
  }

 private:
  std::shared_ptr<arrow::csv::StreamingReader> reader_;

  std::shared_ptr<arrow::RecordBatch> current_batch_;
  size_t current_batch_offset_;
  size_t current_batch_size_;

  std::mutex lock_;
};

class CSVTableReader : public CSVReaderBase {
 public:
  CSVTableReader(const std::string& filename,
                 const arrow::csv::ConvertOptions& convert_options,
                 const arrow::csv::ReadOptions& read_options,
                 const arrow::csv::ParseOptions& parse_options)
      : batch_(nullptr), batch_offset_(0), batch_size_(0) {
    auto file = arrow::io::ReadableFile::Open(filename).ValueOrDie();
    auto table_reader = arrow::csv::TableReader::Make(
                            arrow::io::default_io_context(), file, read_options,
                            parse_options, convert_options)
                            .ValueOrDie();
    std::shared_ptr<arrow::Table> table = table_reader->Read().ValueOrDie();
    arrow::TableBatchReader batch_reader(*table);
    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      auto status = batch_reader.ReadNext(&batch);
      CHECK(status.ok());
      if (batch == nullptr) {
        break;
      }
      batches_.push_back(std::move(batch));
    }
  }

  std::shared_ptr<arrow::RecordBatch> Read(
      size_t limit = std::numeric_limits<size_t>::max()) override {
    std::lock_guard<std::mutex> lock(lock_);
    while (batch_ == nullptr || batch_offset_ == batch_size_) {
      if (batches_.empty()) {
        return nullptr;
      }
      batch_ = batches_.front();
      batches_.pop_front();
      batch_offset_ = 0;
      batch_size_ = batch_->num_rows();
    }
    auto ret = batch_->Slice(batch_offset_,
                             std::min(limit, batch_size_ - batch_offset_));
    batch_offset_ += ret->num_rows();
    return ret;
  }

 private:
  std::deque<std::shared_ptr<arrow::RecordBatch>> batches_;

  std::shared_ptr<arrow::RecordBatch> batch_;
  size_t batch_offset_;
  size_t batch_size_;

  std::mutex lock_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_H_