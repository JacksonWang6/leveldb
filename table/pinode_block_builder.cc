#include "pinode_block_builder.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "util/coding.h"
#include "util/hash.h"

namespace leveldb {

PinodeBlockBuilder::PinodeBlockBuilder(const Options* options, size_t num)
    : options_(options) {
  // 根据数目决定有多少个桶
  bucket_num_ = num / kMaxBucketEntryNum;
  // 判断 load factor
  if (static_cast<double>(num) /
          static_cast<double>(bucket_num_ * kMaxBucketEntryNum) >
      0.8) {
    // load factor太大了，为了避免可能造成的扩容，直接将bucket数目翻一番
    bucket_num_ *= 2;
  }
  buffers_ = new std::string[bucket_num_];
  nums_ = new uint32_t[bucket_num_];
  memset(nums_, 0, sizeof(uint32_t) * bucket_num_);
}

/**
 * key的格式：user_key | seqnum | type
 * value的格式：value
 */
void PinodeBlockBuilder::Add(const Slice& key, const Slice& value) {
  uint32_t hash = Hash(key.data(), key.size());
  uint32_t bucket_idx = hash % bucket_num_;
  std::string& s = buffers_[bucket_idx];
  PutVarint32(&s, key.size());
  PutVarint32(&s, value.size());
  s.append(key.data(), key.size());
  s.append(value.data(), value.size());
  nums_[bucket_idx]++;
}

/**
 * 格式：
 * 1. 首先是N个bucket，bucket的个数和键值对的数量挂钩
 * 2. 然后是元数据字段，一共有N个，对应前面的N个bucket，存储offset和这个bucket中元素数目
 * 3. 最后是bucket的数目
 */
Slice PinodeBlockBuilder::Finish() {
  for (int i = 0; i < bucket_num_; i++) {
    buffer_.append(buffers_[i]);
  }
  uint32_t offset = 0;
  for (int i = 0; i < bucket_num_; i++) {
    PutFixed32(&buffer_, offset);
    PutFixed32(&buffer_, nums_[i]);
    offset += buffers_[i].size();
  }
  PutFixed32(&buffer_, bucket_num_);
  return Slice(buffer_);
}

void PinodeBlockBuilder::Reset() {
  delete buffers_;
  buffer_.clear();
}

}  // namespace leveldb
