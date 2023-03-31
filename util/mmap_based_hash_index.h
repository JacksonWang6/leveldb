#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <unistd.h>

#include "leveldb/slice.h"

#include "util/coding.h"
#include "util/hash.h"
#include "util/my_logging.h"

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t ENTRY_NUM = 8;
constexpr uint32_t BUCKET_SIZE = 8;
constexpr uint32_t KEY_SIZE = 4;
constexpr uint32_t VALUE_SIZE = 4;

using namespace std;

constexpr static uint64_t MASK[] = {
    0,
    0x1ULL,
    0x3ULL,
    0x7ULL,
    0xfULL,
    0x1fULL,
    0x3fULL,
    0x7fULL,
    0xffULL,
    0x1ffULL,
    0x3ffULL,
    0x7ffULL,
    0xfffULL,
    0x1fffULL,
    0x3fffULL,
    0x7fffULL,
    0xffffULL,
    0x1ffffULL,
    0x3ffffULL,
    0x7ffffULL,
    0xfffffULL,
    0x1fffffULL,
    0x3fffffULL,
    0x7fffffULL,
    0xffffffULL,
    0x1ffffffULL,
    0x3ffffffULL,
    0x7ffffffULL,
    0xfffffffULL,
    0x1fffffffULL,
    0x3fffffffULL,
    0x7fffffffULL,
    0xffffffffULL,
    0x1ffffffffULL,
    0x3ffffffffULL,
    0x7ffffffffULL,
    0xfffffffffULL,
    0x1fffffffffULL,
    0x3fffffffffULL,
    0x7fffffffffULL,
    0xffffffffffULL,
    0x1ffffffffffULL,
    0x3ffffffffffULL,
    0x7ffffffffffULL,
    0xfffffffffffULL,
    0x1fffffffffffULL,
    0x3fffffffffffULL,
    0x7fffffffffffULL,
    0xffffffffffffULL,
    0x1ffffffffffffULL,
    0x3ffffffffffffULL,
    0x7ffffffffffffULL,
    0xfffffffffffffULL,
    0x1fffffffffffffULL,
    0x3fffffffffffffULL,
    0x7fffffffffffffULL,
    0xffffffffffffffULL,
    0x1ffffffffffffffULL,
    0x3ffffffffffffffULL,
    0x7ffffffffffffffULL,
    0xfffffffffffffffULL,
    0x1fffffffffffffffULL,
    0x3fffffffffffffffULL,
    0x7fffffffffffffffULL,
};

/**
 * 服务于L0Log的基于mmap的哈希索引
 */

namespace leveldb {

// 每个entry8B，包含4B的key fp和4B的value addr
struct Entry {
  uint32_t fp;
  uint32_t value_addr;
};

// 每个Bucket为64B，当这个Bucket溢出的时候，最后一个entry里面存储溢出桶的地址
struct MMHashBucket {
  Entry entries[ENTRY_NUM];
};

// MMHash索引结构体
struct MMHashIndex {
  uint64_t num_buckets;     // 哈希桶数量
  uint64_t overflow_index;  // 溢出区起始偏移量
  uint64_t bitmap_size;     // 溢出区位图大小
};

static void die(std::string msg) {
  perror(msg.c_str());
  exit(EXIT_FAILURE);
}

// MMHash类
class MMHash {
 public:
  MMHash(string filename, uint64_t size, double normal_ratio)
      : filename_(filename),
        normal_ratio_(normal_ratio),
        fd_(-1),
        mmap_ptr_(nullptr),
        index_(nullptr),
        bitmap_(nullptr) {
    // 首先检查size大小，必须是4KB的整数倍
    size_ = roundUpTo4KB(size);

    // 然后根据size的大小计算出bucket的数目
    num_buckets_ = size_ / sizeof(MMHashBucket);
    num_normal_buckets_ = num_buckets_ * normal_ratio;
    num_overflow_buckets_ = num_buckets_ - num_normal_buckets_;
    overflow_index_ = num_normal_buckets_;

    // 计算溢出区位图大小
    bitmap_size_ = ((num_overflow_buckets_ + 63) / 64) * 8;
    // 第一个4KB是元数据
    size_ += 4096;

    // 按照指定大小创建或打开文件
    fd_ = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ == -1) {
      die("open");
    }
    // 调整文件大小为size
    if (lseek(fd_, size_ - 1, SEEK_SET) == -1) {
      die("lseek");
    }
    if (write(fd_, "", 1) == -1) {
      die("write");
    }

    // 初始化索引
    index_ = static_cast<MMHashIndex*>(mmap(nullptr, sizeof(MMHashIndex),
                                            PROT_READ | PROT_WRITE, MAP_SHARED,
                                            fd_, 0));
    if (index_ == MAP_FAILED) {
      die("mmap");
    }
    index_->num_buckets = num_buckets_;
    index_->overflow_index = overflow_index_;
    index_->bitmap_size = bitmap_size_;

    // 初始化哈希桶
    mmap_ptr_ = static_cast<MMHashBucket*>(
        mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_,
             4096));  // 偏移量从4096开始
    if (mmap_ptr_ == MAP_FAILED) {
      die("mmap");
    }
    memset(mmap_ptr_, 0, size_);

    // 初始化溢出区Bitmap
    bitmap_ = static_cast<uint64_t*>(mmap(nullptr, bitmap_size_,
                                          PROT_READ | PROT_WRITE, MAP_SHARED,
                                          fd_, sizeof(MMHashIndex)));
    if (bitmap_ == MAP_FAILED) {
      die("mmap");
    }
    _n = (num_overflow_buckets_ + 63) / 64;
    int tmp = num_overflow_buckets_ % 64;
    memset(bitmap_, 0, _n * sizeof(uint64_t));
    if (tmp != 0) {
      bitmap_[_n - 1] |= (MASK[64 - tmp] << tmp);
    }

    // 输出调试信息
    LOG_INFO("num_normal_buckets_ = %lu", num_normal_buckets_);
    LOG_INFO("num_overflow_buckets_ = %lu", num_overflow_buckets_);
    LOG_INFO("overflow_index_ = %lu", overflow_index_);
    LOG_INFO("bitmap_size_ = %lu", bitmap_size_);
  }

  // 插入键值对
  void insert(Slice& key, uint32_t value_addr) {
    uint32_t fp = Hash(key.data(), key.size());
    uint64_t pinode = DecodeFixed64(key.data());
    std::string fname(key.data() + 8, key.size() - 8);
    // 计算哈希桶索引和键在哈希桶中的偏移量
    uint64_t bucket_index = fp % num_buckets_;
    // 首先判断这个键是否存在，存在则更新，不存在则插入
    bool updated = try_update(bucket_index, pinode, fname, fp, value_addr);
    if (updated) return;

    // 不存在，那么需要插入
    int entry_index = find_free_entry(bucket_index);
    if (entry_index == -1) {
      // 当前Bucket满了，需要找下一个溢出桶
      int index;
      bool succ = SetFirstFreePos(&index);
      assert(succ);  // 失败说明溢出区满了，那就寄了
      mmap_ptr_[bucket_index].entries[ENTRY_NUM - 1].value_addr =
          overflow_index_ + index;
      // 将键值对插入溢出桶
      insert_overflow_bucket(index+overflow_index_, fp, value_addr);
    } else if (entry_index == -2) {
      // 当前Bucket有下一个溢出桶，去下一个溢出桶进行遍历
      insert_overflow_bucket(
          mmap_ptr_[bucket_index].entries[ENTRY_NUM - 1].value_addr, fp,
          value_addr);
    } else {
      // 找到一个空闲的桶，写入数据
      mmap_ptr_[bucket_index].entries[entry_index].fp = fp;
      mmap_ptr_[bucket_index].entries[entry_index].value_addr = value_addr;
    }
  }

  // 查找键对应的值
  bool find(Slice& key, std::string* res) {
    uint32_t fp = Hash(key.data(), key.size());
    uint64_t pinode = DecodeFixed64(key.data());
    std::string fname(key.data() + 8, key.size() - 8);

    // 计算哈希桶索引和键在哈希桶中的偏移量
    uint64_t bucket_index = fp % num_buckets_;

    return try_find(bucket_index, pinode, fname, fp, res);
  }

  bool remove(Slice& key) {
    uint32_t fp = Hash(key.data(), key.size());
    uint64_t pinode = DecodeFixed64(key.data());
    std::string fname(key.data() + 8, key.size() - 8);

    // 计算哈希桶索引和键在哈希桶中的偏移量
    uint64_t bucket_index = fp % num_buckets_;

    return try_remove(-1, bucket_index, pinode, fname, fp);
  }

 private:
  // 查找哈希桶中的空闲位置
  int find_free_entry(uint64_t bucket_index) {
    int i = 0;
    for (; i < ENTRY_NUM - 1; ++i) {
      if (mmap_ptr_[bucket_index].entries[i].fp == 0 &&
          mmap_ptr_[bucket_index].entries[i].value_addr == 0) {
        return i;
      }
    }
    // 当前Bucket满了，并且没有下一个溢出桶，需要找下一个溢出桶
    if (mmap_ptr_[bucket_index].entries[ENTRY_NUM - 1].value_addr == 0)
      return -1;
    // 当前Bucket有下一个溢出桶，去下一个溢出桶进行遍历
    return -2;
  }

 private:
  string filename_;                // 文件名
  uint64_t size_;                  // 文件大小，4KB的倍数
  double normal_ratio_;            // 普通区占比
  uint64_t num_buckets_;           // 哈希桶数量
  uint64_t num_normal_buckets_;    // 普通区哈希桶数量
  uint64_t num_overflow_buckets_;  // 溢出区哈希桶数量
  uint64_t overflow_index_;        // 溢出区起始偏移量
  uint64_t bitmap_size_;           // 溢出区位图大小
  int fd_;                         // 文件描述符
  MMHashBucket* mmap_ptr_;         // 内存映射指针
  MMHashIndex* index_;             // 索引指针
  uint64_t* bitmap_;               // 溢出区位图指针
  uint32_t _n;

  size_t roundUpTo4KB(size_t size) {
    const size_t KB = 1024;
    const size_t pageSize = 4 * KB;
    return (size + pageSize - 1) & ~(pageSize - 1);
  }

  /**
   * 如果要插入的这个键已经存在，那么更新它，并且返回true
   */
  bool try_update(int index, uint64_t pinode, std::string& fname, uint32_t fp,
                  uint32_t value_addr) {
    for (int i = 0; i < ENTRY_NUM - 1; i++) {
      if (mmap_ptr_[index].entries[i].fp == fp) {
        // TODO 根据addr从L0Log把键值对读上来进行比较，如果相同，那么更新
        bool equal = true;
        if (equal) {
          // TODO: update
          return true;
        }
      }
    }
    if (mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr != 0) {
      // 存在下一个溢出桶
      return try_update(mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr,
                        pinode, fname, fp, value_addr);
    }
    return false;
  }

  /**
   * 如果要查找的这个键已经存在，那么返回结果
   */
  bool try_find(int index, uint64_t pinode, std::string& fname, uint32_t fp,
                std::string* res) {
    for (int i = 0; i < ENTRY_NUM - 1; i++) {
      if (mmap_ptr_[index].entries[i].fp == fp) {
        // TODO：根据addr从L0Log把键值对读上来进行比较，如果相同，那么找到了，则将结果复制到res中
        bool equal = true;
        if (equal) {
          // TODO: 复制结果
          return true;
        }
      }
    }
    if (mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr != 0) {
      // 在下一个溢出桶中继续查找
      return try_find(mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr,
                      pinode, fname, fp, res);
    }
    return false;
  }

  bool try_remove(int parent, int index, uint64_t pinode, std::string& fname,
                  uint32_t fp) {
    for (int i = 0; i < ENTRY_NUM - 1; i++) {
      if (mmap_ptr_[index].entries[i].fp == fp) {
        // TODO：根据addr从L0Log把键值对读上来进行比较，如果相同，那么找到了，那么就可以删除
        bool equal = true;
        if (equal) {
          mmap_ptr_[index].entries[i].fp = 0;
          mmap_ptr_[index].entries[i].value_addr = 0;
          // 如果这个桶是溢出桶，还需要判断是否可以回收
          if (index >= overflow_index_) {
            int k = 0;
            for (; k < ENTRY_NUM - 1; k++) {
              if (mmap_ptr_[index].entries[k].fp != 0 ||
                  mmap_ptr_[index].entries[i].value_addr != 0) {
                break;
              }
            }
            if (k == ENTRY_NUM - 1) {
              // 这个溢出桶可以回收了
              Clear(index - overflow_index_);
              if (parent != -1) {
                // 被回收了，那么它的前一个桶的指向下一个溢出桶的地址也得改变
                mmap_ptr_[parent].entries[ENTRY_NUM - 1].value_addr =
                    mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr;
              }
            }
          }
          return true;
        }
      }
    }
    if (mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr != 0) {
      // 在下一个溢出桶中继续查找
      return try_remove(index,
                        mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr,
                        pinode, fname, fp);
    }
    return false;
  }

  /**
   * 实现了将键值对插入溢出区的逻辑，溢出区也可能接着溢出
   */
  void insert_overflow_bucket(int index, uint32_t fp, uint32_t value_addr) {
    int entry_index = find_free_entry(index);
    if (entry_index == -1) {
      // 当前Bucket满了，需要找下一个溢出桶
      int index;
      bool succ = SetFirstFreePos(&index);
      assert(succ);  // 失败说明溢出区满了，那就寄了
      mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr =
          overflow_index_ + index;
      // 将键值对插入溢出桶
      insert_overflow_bucket(index, fp, value_addr);
    } else if (entry_index == -2) {
      // 当前Bucket有下一个溢出桶，去下一个溢出桶进行遍历
      insert_overflow_bucket(mmap_ptr_[index].entries[ENTRY_NUM - 1].value_addr,
                             fp, value_addr);
    } else {
      // 找到一个空闲的桶，写入数据
      mmap_ptr_[index].entries[entry_index].fp = fp;
      mmap_ptr_[index].entries[entry_index].value_addr = value_addr;
    }
  }

  // for bitmap
  bool Full() const {
    for (uint32_t i = 0; i < _n; i++) {
      if (~bitmap_[i]) {
        return false;
      }
    }
    return true;
  }

  // return the first free position of bitmap, return -1 if full
  int FirstFreePos() const {
    for (uint32_t i = 0; i < _n; i++) {
      int ffp = ffsl(~bitmap_[i]) - 1;
      if (ffp != -1) {
        return i * 64 + ffp;
      }
    }
    return -1;
  }

  bool SetFirstFreePos(int* index) {
    *index = FirstFreePos();
    if (*index == -1) {
      return false;
    }
    assert(*index < num_overflow_buckets_);
    int n = *index / 64;
    int off = *index % 64;
    bitmap_[n] |= (1ULL << off);
    return true;
  }

  bool Test(int index) const {
    assert(index < num_overflow_buckets_);
    int n = index / 64;
    int off = index % 64;
    return bitmap_[n] & (1ULL << off);
  }

  void Clear(int index) {
    assert(index < num_overflow_buckets_);
    int n = index / 64;
    int off = index % 64;
    bitmap_[n] &= ~(1ULL << off);
  }

  bool Set(int index) {
    assert(index < num_overflow_buckets_);
    int n = index / 64;
    int off = index % 64;
    if (bitmap_[n] & (1ULL << off)) {
      return false;
    }
    bitmap_[n] |= (1ULL << off);
    return true;
  }
};

}  // namespace leveldb
