#include "hstable_builder.h"

#include <cassert>

#include "port/port_stdcxx.h"
#include "table/format.h"
#include "table/pinode_block_builder.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct HSTableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt), file(f), offset(0), closed(false) {}

  Options options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  bool closed;  // Either Finish() or Abandon() has been called.

  std::string compressed_output;
};

HSTableBuilder::HSTableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {}

HSTableBuilder::~HSTableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_;
}

Status HSTableBuilder::status() const { return rep_->status; }

/**
 * 生成一个HSTable的时候调用，由于需要提前知道一个block需要的bucket数目
 * 所以在这里一次性传入需要进行合并的block当中一个pinode对应的所有键值对数据
 */
void HSTableBuilder::Add(uint64_t pinode, const std::vector<Slice> key,
                         const std::vector<Slice> value) {
  assert(key.size() == value.size());
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  PinodeBlockBuilder builder(&rep_->options, key.size());
  for (int i = 0; i < key.size(); i++) {
    builder.Add(key[i], value[i]);
  }

  BlockHandle handle;
  // block已经构建完了，直接刷盘
  WriteBlock(&builder, &handle);
  // 更新table文件大小
  size_ += handle.size();
  // 记录pinode -> handle的信息，等落盘后写入mapping table
  pinode2handle.emplace(pinode, handle);
}

void HSTableBuilder::WriteBlock(PinodeBlockBuilder* block,
                                BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void HSTableBuilder::WriteRawBlock(const Slice& block_contents,
                                   CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

/**
 * 上层调用者自己通过CurrentSizeEstimate判断是否超过了阈值，超过的话就调用Finish，然后重新开启一个HSTable
 */
Status HSTableBuilder::Finish() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
  return r->status;
}

uint32_t HSTableBuilder::CurrentSizeEstimate() const { return size_; }

}  // namespace leveldb
