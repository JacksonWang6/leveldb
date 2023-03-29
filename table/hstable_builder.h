#pragma once

#include <unordered_map>

#include "leveldb/env.h"
#include "leveldb/options.h"

#include "table/format.h"
#include "table/pinode_block_builder.h"

namespace leveldb {

/**
 * 由很多个data block组成，一个data block里面存放一个pinode的数据
 */
class HSTableBuilder {
 public:
  HSTableBuilder(const Options& options, WritableFile* file);

  HSTableBuilder(const HSTableBuilder&) = delete;
  HSTableBuilder& operator=(const HSTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~HSTableBuilder();

  // Return non-ok iff some error has been detected.
  Status status() const;
  void Add(uint64_t pinode, const std::vector<Slice> key,
           const std::vector<Slice> value);
  Status Finish();

  uint32_t CurrentSizeEstimate() const;

  const std::unordered_map<uint64_t, BlockHandle>& GetPinode2HandleMap() {
    return pinode2handle;
  }

 private:
  bool ok() const { return status().ok(); }
  void WriteBlock(PinodeBlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);
  uint32_t size_ = 0;
  std::unordered_map<uint64_t, BlockHandle> pinode2handle;

  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb
