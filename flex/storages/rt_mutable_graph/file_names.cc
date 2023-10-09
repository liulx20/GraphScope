#include "flex/storages/rt_mutable_graph/file_names.h"

namespace gs {

std::string schema_path(const std::string& work_dir) {
  return data_dir + "/schema";
}

std::string snapshots_dir(const std::string& work_dir) {
  return data_dir + "/snapshots";
}

std::string snapshot_version_path(const std::string& work_dir) {
  return snapshots_dir(work_dir) + "/VERSION";
}

uint32_t get_snapshot_version(const std::string& work_dir) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "rb");
  uint32_t version = 0;
  fread(&version, sizeof(uint32_t), 1, version_file);
  fclose(version_file);
  return version;
}

void set_snapshot_version(const std::string& work_dir, uint32_t version) {
  std::string version_path = snapshot_version_path(work_dir);
  FILE* version_file = fopen(version_path.c_str(), "wb");
  fwrite(&version, sizeof(uint32_t), 1, version_file);
  fflush(version_file);
  fclose(version_file);
}

std::string snapshot_dir(const std::string& work_dir, uint32_t version) {
  return snapshots_dir(work_dir) + "/" + std::to_string(version);
}

std::string wal_dir(const std::string& work_dir) { return data_dir + "/wal"; }

std::string runtime_dir(const std::string& work_dir) {
  return data_dir + "/runtime";
}

}  // namespace gs