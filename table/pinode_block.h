#pragma once

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include "table/format.h"

namespace leveldb {

class PinodeBlock {
 public:
  // Initialize the block with the specified contents.
  explicit PinodeBlock(const BlockContents& contents, Comparator* comparator);

  PinodeBlock(const PinodeBlock&) = delete;
  PinodeBlock& operator=(const PinodeBlock&) = delete;

  ~PinodeBlock();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);
  Status Get(const Slice& key, std::string* value);

 private:
  class Iter;
  int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  const char* data_;
  size_t size_;
  bool owned_;  // Block owns data_[]

  uint32_t bucket_num_;
  uint32_t* offsets_;
  uint32_t* nums_;

  const Comparator* const comparator_;
};

}  // namespace leveldb
