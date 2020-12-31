#include "terarkfs_env.h"
namespace rocksdb {

TerarkfsEnv::TerarkfsEnv(Env* base_env) : EnvWrapper(base_env) {}

int TerarkfsEnv::initialize(const char* config_file_path) {
  storage_.reset(new masse::masse_storage());
  return storage_->open(config_file_path);
}

TerarkfsEnv::~TerarkfsEnv() { storage_->close(); }

Status TerarkfsEnv::NewSequentialFile(const std::string& fname,
                                      std::unique_ptr<SequentialFile>* result,
                                      const EnvOptions& options) {
  auto* file = storage_->new_masse_readable_file(fname);
  if (file == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsSequentialFile(file, options));
  return Status::OK();
}

Status TerarkfsEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  auto* file = storage_->new_masse_readable_file(fname);
  if (file == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsRandomAccessFile(file, options));
  return Status::OK();
}

Status TerarkfsEnv::NewWritableFile(const std::string& fname,
                                    std::unique_ptr<WritableFile>* result,
                                    const EnvOptions& options) {
  auto* file = storage_->new_masse_writable_file(fname);
  if (file == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsWritableFile(file, options));
  return Status::OK();
}

Status TerarkfsEnv::NewDirectory(const std::string& name,
                                 std::unique_ptr<Directory>* result) {
  auto* meta = storage_->new_masse_folder(name);
  if (meta == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsDirectory(meta->m_file_id));
  return Status::OK();
}

Status TerarkfsEnv::FileExists(const std::string& fname) {
  return Status::NotSupported();
}

Status TerarkfsEnv::GetChildren(const std::string& dir,
                                std::vector<std::string>* result) {
  return Status::NotSupported();
}

Status TerarkfsEnv::DeleteFile(const std::string& fname) {
  return Status::NotSupported();
}

Status TerarkfsEnv::Truncate(const std::string& fname, size_t size) {
  return Status::NotSupported();
}

Status TerarkfsEnv::CreateDir(const std::string& dirname) {
  return Status::NotSupported();
}

Status TerarkfsEnv::CreateDirIfMissing(const std::string& dirname) {
  return Status::NotSupported();
}

Status TerarkfsEnv::DeleteDir(const std::string& dirname) {
  return Status::NotSupported();
}

Status TerarkfsEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
  return Status::NotSupported();
}

Status TerarkfsEnv::GetFileModificationTime(const std::string& fname,
                                            uint64_t* time) {
  return Status::NotSupported();
}

Status TerarkfsEnv::RenameFile(const std::string& src,
                               const std::string& target) {
  return Status::NotSupported();
}

Status TerarkfsEnv::LinkFile(const std::string& src,
                             const std::string& target) {
  return Status::NotSupported();
}

Status TerarkfsEnv::NewLogger(const std::string& fname,
                              std::shared_ptr<Logger>* result) {
  return Status::NotSupported();
}

Status TerarkfsEnv::LockFile(const std::string& fname, FileLock** flock) {
  return Status::NotSupported();
}

Status TerarkfsEnv::UnlockFile(FileLock* flock) {
  return Status::NotSupported();
}

}  // namespace rocksdb
