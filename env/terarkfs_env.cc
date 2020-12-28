#include "terarkfs_env.h"
namespace rocksdb {

class TerarkfsEnv {
 public:
  explicit TerarkfsEnv(Env* base_env) {}

  int initialize(const char* config_file_path) {
    storage_.reset(new masse::masse_storage());
    return storage_->open(config_file_path);
  }

  ~TerarkfsEnv() {
    storage_->close();
  }

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) {
    auto* file = storage_->new_masse_readable_file(fname);
    if (file == nullptr) {
      return Status::Incomplete();
    }
    result->reset(new TerarkfsSequentialFile(file, options));
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) {
    auto* file = storage_->new_masse_readable_file(fname);
    if (file == nullptr) {
      return Status::Incomplete();
    }
    result->reset(new TerarkfsRandomAccessFile(file, options));
    return Status::OK();
  }

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) {
    auto* file = storage_->new_masse_writable_file(fname);
    if (file == nullptr) {
      return Status::Incomplete();
    }
    result->reset(new TerarkfsWritableFile(file, options));
    return Status::OK();                         
  }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) {}

  Status FileExists(const std::string& fname) {}

  Status GetChildren(const std::string& dir, std::vector<std::string>* result) {
      
  }

  Status DeleteFile(const std::string& fname) {}

  Status Truncate(const std::string& fname, size_t size) {}

  Status CreateDir(const std::string& dirname) {}

  Status CreateDirIfMissing(const std::string& dirname) {}

  Status DeleteDir(const std::string& dirname) {}

  Status GetFileSize(const std::string& fname, uint64_t* file_size) {}

  Status GetFileModificationTime(const std::string& fname, uint64_t* time) {}

  Status RenameFile(const std::string& src, const std::string& target) {}

  Status LinkFile(const std::string& src, const std::string& target) {}

  Status NewLogger(const std::string& fname, std::shared_ptr<Logger>* result) {}

  Status LockFile(const std::string& fname, FileLock** flock) {}

  Status UnlockFile(FileLock* flock) {}

  Status GetTestDirectory(std::string* path) {}

  // Results of these can be affected by FakeSleepForMicroseconds()
  Status GetCurrentTime(int64_t* unix_time) {}
  uint64_t NowMicros() {}
  uint64_t NowNanos() {}

  Status CorruptBuffer(const std::string& fname);

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()
  void FakeSleepForMicroseconds(int64_t micros);

 private:
  std::unique_ptr<masse::masse_storage> storage_;
  std::string NormalizePath(const std::string path);
};

}  // namespace rocksdb
