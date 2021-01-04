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

Status TerarkfsEnv::NewDirectory(const std::string& dirname,
                                 std::unique_ptr<Directory>* result) {
  if (dirname.empty()) {
    return Status::InvalidArgument();
  }
  std::string dir = dirname;
  if (dirname.back() != '/') {
    dir += '/';
  }                           
  auto* meta = storage_->new_masse_folder(dir);
  if (meta == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsDirectory(meta->m_file_id));
  return Status::OK();
}

Status TerarkfsEnv::FileExists(const std::string& fname) {
  size_t ret = size_t(-1);
  if ((ret = storage_->get_file_size(fname)) == size_t(-1)) {
    return Status::NotFound();
  }
  return Status::OK();
}

Status TerarkfsEnv::GetChildren(const std::string& dirname,
                                std::vector<std::string>* result) {
  if (dirname.empty()) {
    return Status::InvalidArgument();
  }
  std::string dir = dirname;
  if (dirname.back() != '/') {
    dir += '/';
  }
  if (storage_->get_children(dir, result) < 0) {
    return Status::Incomplete();
  }
  return Status::OK();
}

Status TerarkfsEnv::DeleteFile(const std::string& fname) {
  if (storage_->delete_file(fname, false) < 0) {
    return Status::Incomplete();
  }
  return Status::OK();
}

Status TerarkfsEnv::Truncate(const std::string& fname, size_t size) {
  return Status::OK();
}

Status TerarkfsEnv::CreateDir(const std::string& dirname) {
  if (dirname.empty()) {
    return Status::InvalidArgument();
  }
  std::string dir = dirname;
  if (dirname.back() != '/') {
    dir += '/';
  }
  auto* meta = storage_->new_masse_folder(dir);
  if (meta == nullptr) {
    return Status::Incomplete();
  }
  return Status::OK();
}

Status TerarkfsEnv::CreateDirIfMissing(const std::string& dirname) {
    if (dirname.empty()) {
    return Status::InvalidArgument();
  }
  std::string dir = dirname;
  if (dirname.back() != '/') {
    dir += '/';
  }
  auto* meta = storage_->new_masse_folder(dir);
  if (meta == nullptr) {
    return Status::Incomplete();
  }
  return Status::OK();
}

Status TerarkfsEnv::DeleteDir(const std::string& dirname) {
  return Status::OK();
}

Status TerarkfsEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
  if ((*file_size = storage_->get_file_size(fname)) == size_t(-1)) {
    return Status::NotFound();
  }
  return Status::OK();
}

Status TerarkfsEnv::GetFileModificationTime(const std::string& fname,
                                            uint64_t* time) {
  return Status::OK();
}

Status TerarkfsEnv::RenameFile(const std::string& src,
                               const std::string& target) {
  if (storage_->rename_file(src, target) < 0) {
    return Status::Incomplete();
  }
  return Status::OK();
}

Status TerarkfsEnv::LinkFile(const std::string& src,
                             const std::string& target) {
  return Status::OK();
}

Status TerarkfsEnv::NewLogger(const std::string& fname,
                              std::shared_ptr<Logger>* result) {
  auto* file = storage_->new_masse_writable_file(fname);
  if (file == nullptr) {
    return Status::Incomplete();
  }
  result->reset(new TerarkfsLogger(file, this));
  return Status::OK();
}

Status TerarkfsEnv::LockFile(const std::string& fname, FileLock** flock) {
  return Status::OK();
}

Status TerarkfsEnv::UnlockFile(FileLock* flock) {
  return Status::OK();
}
}  // namespace rocksdb
