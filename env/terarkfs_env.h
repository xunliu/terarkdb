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
#include "sys/time.h"

namespace rocksdb {

class TerarkfsSequentialFile : public SequentialFile {
 private:
  std::shared_ptr<masse::masse_readable_file> file_;
  size_t offset_ = 0;

 public:
  TerarkfsSequentialFile(masse::masse_readable_file* file,
                         const EnvOptions& options)
      : file_(file) {}

  virtual ~TerarkfsSequentialFile() {
    if (file_) {
      file_->close();
      file_.reset();
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) override {
    ssize_t r;
    if ((r = file_->pread(scratch, n, offset_, nullptr, true)) < 0) {
      Status::Corruption();
      file_->pread(scratch, n, offset_, nullptr, true);
      return Status::Corruption();
    }
    *result = Slice(scratch, r);
    offset_ += r;
    return Status::OK();
  }

  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                                char* scratch) override {
    ssize_t r;
    if ((r = file_->pread(scratch, n, offset, nullptr, true)) < 0) {
      Status::Corruption();
      file_->pread(scratch, n, offset, nullptr, true);
      return Status::Corruption();
    }
    *result = Slice(scratch, r);
    return Status::OK();
  }

  virtual Status Skip(uint64_t n) override {
    offset_ += n;
    return Status::OK();
  }

  virtual bool use_direct_io() const override { return false; }

  virtual size_t GetRequiredBufferAlignment() const override {
    return 1;  // masse::MASSE_WRITE_ALIGN_SIZE;
  }

  size_t GetTerarkfsFileID() const { return file_->m_file_meta->m_meta_addr; }
};

class TerarkfsRandomAccessFile : public RandomAccessFile {
 private:
  std::shared_ptr<masse::masse_readable_file> file_;

 public:
  TerarkfsRandomAccessFile(masse::masse_readable_file* file,
                           const EnvOptions& options)
      : file_(file) {}

  virtual ~TerarkfsRandomAccessFile() {
    if (file_) {
      file_->close();
      file_.reset();
    }
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const final {
    ssize_t r;
    if ((r = file_->pread(scratch, n, offset, nullptr, true)) < 0) {
      auto print_info = [&]() {
        fprintf(stderr, "TREF: %20s, %8zd, %8zd, %8zd\n",
                file_->m_file_meta->m_file_name, r, offset, n);
      };
      print_info();
      Status::Corruption();
      r = file_->pread(scratch, n, offset, nullptr, true);
      print_info();
      return Status::Corruption();
    }
    *result = Slice(scratch, r);
    return Status::OK();
  }

  virtual Status Prefetch(uint64_t offset, size_t n) override {
    return Status::OK();
  }

  virtual bool use_aio_reads() const final { return false; }

  virtual bool use_direct_io() const final { return false; }

  virtual size_t GetRequiredBufferAlignment() const final {
    return 1;  // masse::MASSE_WRITE_ALIGN_SIZE;
  }

  size_t GetTerarkfsFileID() const { return file_->m_file_meta->m_file_id; }
};

class TerarkfsWritableFile : public WritableFile {
 private:
  std::shared_ptr<masse::masse_writable_file> file_;

 public:
  explicit TerarkfsWritableFile(masse::masse_writable_file* file,
                                const EnvOptions& options)
      : file_(file) {}

  virtual ~TerarkfsWritableFile() { Close(); }

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

  virtual Status Sync() override {
    file_->flush(true, true);
    return Status::OK();
  }

  virtual Status Fsync() override {
    file_->flush(true, false);
    return Status::OK();
  }

  virtual Status Flush() override {
    // Insignificant cached size of file object comparing with kernel's
    // implementation. file_->flush(false, false);
    return Status::OK();
  }

  virtual Status Close() override {
    if (file_) {
      file_->flush(true, true);
      file_.reset();
    }
    return Status::OK();
  }

  virtual uint64_t GetFileSize() override {
    return file_->m_file_meta->m_file_size;
  }

  virtual bool IsSyncThreadSafe() const override { return true; }

  virtual size_t GetRequiredBufferAlignment() const final {
    return 1;  // masse::MASSE_WRITE_ALIGN_SIZE;
  }

  virtual bool use_direct_io() const final { return false; }

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

class TerarkfsLogger : public Logger {
 private:
  Status TerarkfsCloseHelper() {
    if (file_ != nullptr) {
      file_->close();
      delete file_;
      file_ = nullptr;
    }
    return Status::OK();
  }

  uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  };

  masse::masse_writable_file* file_;
  std::atomic_size_t log_size_;
  int fd_;
  const static uint64_t flush_every_seconds_ = 5;
  std::atomic_uint_fast64_t last_flush_micros_;
  Env* env_;
  std::atomic<bool> flush_pending_;

 protected:
  virtual Status CloseImpl() override { return TerarkfsCloseHelper(); }

 public:
  TerarkfsLogger(masse::masse_writable_file* f, Env* env,
                 const InfoLogLevel log_level = InfoLogLevel::ERROR_LEVEL)
      : Logger(log_level),
        file_(f),
        log_size_(0),
        fd_(f->m_file_meta->m_file_id),
        last_flush_micros_(0),
        env_(env),
        flush_pending_(false) {}

  virtual ~TerarkfsLogger() {
    if (!closed_) {
      closed_ = true;
      TerarkfsCloseHelper();
    }
  }

  virtual void Flush() override {
    if (flush_pending_) {
      flush_pending_ = false;
      file_->flush(false);
    }
    last_flush_micros_ = env_->NowMicros();
  }

  using Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {
    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a larger dynamically allocated buffer.
    char buffer[500];
    ptrdiff_t bufsize = sizeof(buffer);
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      if (iter == 0) {
        base = buffer;
      } else {
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(gettid()));

      // Print the message
      va_list backup_ap;
      va_copy(backup_ap, ap);
      p += vsnprintf(p, p < limit ? (limit - p) : 0, format, backup_ap);
      va_end(backup_ap);

      // Truncate to available space if necessary
      if (p >= limit) {
        assert(iter == 0);
        bufsize = p + 1 - base;
        continue;  // try again with larger buffer
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      const size_t write_size = p - base;

      size_t sz = file_->write(base, write_size);
      flush_pending_ = true;
      if (sz > 0) {
        log_size_ += write_size;
      }
      uint64_t now_micros =
          static_cast<uint64_t>(now_tv.tv_sec) * 1000000 + now_tv.tv_usec;
      if (now_micros - last_flush_micros_ >= flush_every_seconds_ * 1000000) {
        Flush();
      }
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }

  size_t GetLogFileSize() const override { return log_size_; }
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

  virtual Status NewDirectory(const std::string& dirname,
                              std::unique_ptr<Directory>* result) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& dirname,
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

  std::string NormalizePath(const std::string& path);
};

}  // namespace rocksdb
