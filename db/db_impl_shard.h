#pragma once

#include "db/db_impl.h"
#include <cassert>

#include "leveldb/status.h"

#include "util/hash.h"
namespace leveldb {

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;
class SharderDBImpl : public DB {
 private:
  DBImpl shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  SharderDBImpl(const SharderDBImpl&) = delete;
  SharderDBImpl& operator=(const SharderDBImpl&) = delete;

  ~SharderDBImpl() override;

  /**
   * TODO: 提取出pinode进行分区的计算
   */
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  // Implementations of the DB interface
  Status Put(const WriteOptions& wo, const Slice& key,
             const Slice& value) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Put(wo, key, value);
  }

  Status Delete(const WriteOptions& wo, const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Delete(wo, key);
  }

  Status Write(const WriteOptions& options, WriteBatch* updates) override {
    // const uint32_t hash = HashSlice(key);
    // return shard_[Shard(hash)].Delete(wo, key);
    return Status::NotSupported("not implemented");
  }

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Get(options, key, value);
  }

  Iterator* NewIterator(const ReadOptions&) override {
    assert(0);
  }
  const Snapshot* GetSnapshot() override {
    assert(0);

  }
  void ReleaseSnapshot(const Snapshot* snapshot) override {
    assert(0);

  }
  bool GetProperty(const Slice& property, std::string* value) override {
    assert(0);

  }
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override {
    assert(0);

  }
  void CompactRange(const Slice* begin, const Slice* end) override {
    assert(0);

  }
};

}  // namespace leveldb
