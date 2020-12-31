//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "masse_storage.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "stdio.h"

namespace rocksdb {

class TerarkfsSequentialFile : public SequentialFile {
 private:
  std::shared_ptr<masse::masse_readable_file> file_;
  size_t offset_ = 0;

 public:
  TerarkfsSequentialFile(masse::masse_readable_file* file,
                         const EnvOptions& options)
      : file_(file) {
    assert(options.use_direct_reads && !options.use_aio_reads);
  }

  virtual ~TerarkfsSequentialFile() { file_->close(); }

  virtual Status Read(size_t n, Slice* result, char* scratch) override {
    if (file_->pread(scratch, n, offset_, nullptr, true) != n) {
      return Status::Corruption();
    }
    *result = Slice(scratch, n);
    offset_ += n;
    return Status::OK();
  }

  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                                char* scratch) override {
    if (file_->pread(scratch, n, offset, nullptr, true) != n) {
      return Status::Corruption();
    }
    *result = Slice(scratch, n);
    return Status::OK();
  }

  virtual Status Skip(uint64_t n) override {
    offset_ += n;
    return Status::OK();
  }

  virtual bool use_direct_io() const override { return false; }

  virtual size_t GetRequiredBufferAlignment() const override {
    return masse::MASSE_WRITE_ALIGN_SIZE;
  }

  size_t GetTerarkfsFileID() const { return file_->m_file_meta->m_file_id; }
};

class TerarkfsRandomAccessFile : public RandomAccessFile {
 private:
  std::shared_ptr<masse::masse_readable_file> file_;

 public:
  TerarkfsRandomAccessFile(masse::masse_readable_file* file,
                           const EnvOptions& options)
      : file_(file) {
    assert(options.use_direct_reads && !options.use_aio_reads);
  }

  virtual ~TerarkfsRandomAccessFile() { file_->close(); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const final {
    if (file_->pread(scratch, n, offset, nullptr, true) != n) {
      return Status::Corruption();
    }
    *result = Slice(scratch, n);
    return Status::OK();
  }

  virtual Status Prefetch(uint64_t offset, size_t n) override {
    return Status::OK();
  }

  virtual bool use_aio_reads() const final { return false; }

  virtual bool use_direct_io() const final { return true; }

  virtual size_t GetRequiredBufferAlignment() const final {
    return masse::MASSE_WRITE_ALIGN_SIZE;
  }

  size_t GetTerarkfsFileID() const { return file_->m_file_meta->m_file_id; }
};

class TerarkfsWritableFile : public WritableFile {
 private:
  std::shared_ptr<masse::masse_writable_file> file_;

 public:
  explicit TerarkfsWritableFile(masse::masse_writable_file* file,
                                const EnvOptions& options)
      : file_(file) {
    assert(options.use_direct_reads && !options.use_aio_reads);
  }

  virtual ~TerarkfsWritableFile() { file_->close(); }

  virtual Status Append(const Slice& data) override {
    if (data.size() != file_->write(data.data(), data.size())) {
      return Status::Corruption();
    }
    return Status::OK();
  }

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return Status::NotSupported();
  }

  virtual Status Truncate(uint64_t size) override {
    return Status::NotSupported();
  }

  virtual Status Sync() override { return Status::OK(); }

  virtual Status Fsync() override {
    file_->flush(false);
    return Status::OK();
  }

  virtual Status Flush() override {
    file_->flush(true);
    return Status::OK();
  }

  virtual Status Close() override {
    file_->close();
    return Status::OK();
  }

  virtual uint64_t GetFileSize() override {
    return file_->m_file_meta->m_file_size;
  }

  virtual bool IsSyncThreadSafe() const override { return true; }

  virtual size_t GetRequiredBufferAlignment() const final {
    return masse::MASSE_WRITE_ALIGN_SIZE;
  }

  virtual bool use_direct_io() const final { return true; }

  size_t GetTerarkfsFileID() const { return file_->m_file_meta->m_file_id; }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    // IMPL IN FUTURE VERSION
  }

  virtual Status Allocate(uint64_t offset, uint64_t len) override {
    // IMPL IN FUTURE VERSION
    return Status::OK();
  }

  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    // IMPL IN FUTURE VERSION
    return Status::OK();
  }
};
class TerarkfsDirectory : public Directory {
  size_t id_;

 public:
  explicit TerarkfsDirectory(size_t id) : id_(id) {}
  virtual Status Fsync() override { return Status::OK(); }
};

class TerarkfsEnv : public EnvWrapper {
 public:
  explicit TerarkfsEnv(Env* base_env);

  int initialize(const char* config_file_path);

  virtual ~TerarkfsEnv();

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override;

  virtual Status NewRandomRWFile(const std::string& fname,
                                 std::unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options) override {
    return Status::NotSupported();
  }

  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) override {
    return Status::NotSupported();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override;

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override;

  virtual Status DeleteFile(const std::string& fname) override;

  virtual Status Truncate(const std::string& fname, size_t size) override;

  virtual Status CreateDir(const std::string& dirname) override;

  virtual Status CreateDirIfMissing(const std::string& dirname) override;

  virtual Status DeleteDir(const std::string& dirname) override;

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override;

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override;

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override;

  virtual Status LockFile(const std::string& fname, FileLock** flock) override;

  virtual Status UnlockFile(FileLock* flock) override;

 private:
  std::unique_ptr<masse::masse_storage> storage_;
  std::string NormalizePath(const std::string path);
};

}  // namespace rocksdb
