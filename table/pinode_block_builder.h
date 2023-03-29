#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/stat.h>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

constexpr size_t kDefaultFnameSize = 20;
constexpr size_t kMaxBucketEntryNum = 32;
constexpr size_t kMaxBucketSize =
    kMaxBucketEntryNum *
    (sizeof(uint64_t) + kDefaultFnameSize + sizeof(uint64_t) +
     sizeof(struct stat));  // 每个桶里面最大数据大小

/**
 * 需要对leveldb做一些侵入式的改动
 *
 * 1.这里块构建需要提前知道数据量的个数，从而初始化一个合适的桶个数
 * 2.每个桶的数据都需要先存放在内存当中，等到构建完成再按照顺序一次性Flush到SSD当中
 *
 * merge操作也在builder中实现，传入的是两个block，将这两个block合并成一个block
 */
class PinodeBlockBuilder {
 public:
  explicit PinodeBlockBuilder(const Options* options, size_t num);

  PinodeBlockBuilder(const PinodeBlockBuilder&) = delete;
  PinodeBlockBuilder& operator=(const PinodeBlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

 private:
  const Options* options_;
  // 桶的数目
  size_t bucket_num_;
  // 缓冲区，根据桶数目动态分配的
  std::string* buffers_;
  uint32_t* nums_;
  std::string buffer_;
};

}  // namespace leveldb
