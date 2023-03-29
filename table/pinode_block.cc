#include "pinode_block.h"

#include "db/dbformat.h"
#include <cassert>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/status.h"

#include "util/coding.h"
#include "util/hash.h"

namespace leveldb {

PinodeBlock::PinodeBlock(const BlockContents& contents, Comparator* comparator)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated),
      comparator_(comparator) {
  // 解析出bucket num
  bucket_num_ = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
  offsets_ = new uint32_t[bucket_num_];
  nums_ = new uint32_t[bucket_num_];
  // 解析出offsets
  for (int i = 0; i < bucket_num_; i++) {
    offsets_[i] = DecodeFixed32(data_ + size_ - sizeof(uint32_t) -
                                2 * sizeof(uint32_t) * (bucket_num_ - i));
    nums_[i] =
        DecodeFixed32(data_ + size_ - 2 * sizeof(uint32_t) * (bucket_num_ - i));
  }
}

PinodeBlock::~PinodeBlock() {
  if (owned_) {
    delete[] data_;
  }
}

static inline const int DecodeEntry(const char* p, const char* limit,
                                    uint32_t* key_length,
                                    uint32_t* value_length) {
  const char* pp = p;
  *key_length = reinterpret_cast<const uint8_t*>(p)[0];
  *value_length = reinterpret_cast<const uint8_t*>(p)[1];
  if ((*key_length | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    pp += 2;
  } else {
    if ((pp = GetVarint32Ptr(pp, limit, key_length)) == nullptr) return -1;
    if ((pp = GetVarint32Ptr(pp, limit, value_length)) == nullptr) return -1;
  }

  if (static_cast<uint32_t>(limit - p) < (*key_length + *value_length)) {
    return -1;
  }

  return static_cast<int>(pp - p);
}

Status PinodeBlock::Get(const Slice& key, std::string* value) {
  uint32_t hash = Hash(key.data(), key.size());
  uint32_t bucket_idx = hash % bucket_num_;
  uint32_t offset = offsets_[bucket_idx];
  uint32_t entry_num = nums_[bucket_idx];

  // 遍历这个bucket当中的每一条键值对
  // 每个键值对的格式是 key_len | val_len | key | value
  // 其中key由三部分构成 user_key | seq_num | type
  for (int i = 0; i < entry_num; i++) {
    uint32_t key_length;
    uint32_t value_length;
    const char* limit =
        data_ + ((i == entry_num - 1) ? size_ : offsets_[i + 1]);
    offset += DecodeEntry(data_ + offset, limit, &key_length, &value_length);
    const char* key_ptr = data_ + offset;
    const char* value_ptr = data_ + offset + key_length;
    assert(key_ptr != nullptr);
    Slice cur_key(key_ptr, key_length);
    if (Compare(cur_key, key) == 0) {
      *value = std::string(value_ptr, value_length);
      return Status::OK();
    }

    offset += key_length;
    offset += value_length;
  }

  return Status::NotFound("");
}

// TODO: 实现迭代器，主要用于merge操作

}  // namespace leveldb
