#ifndef GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_
#define GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_

namespace gs {

std::string schema_path(const std::string& work_dir);

std::string snapshots_dir(const std::string& work_dir);

std::string snapshot_version_path(const std::string& work_dir);

uint32_t get_snapshot_version(const std::string& work_dir);

void set_snapshot_version(const std::string& work_dir, uint32_t version);

std::string snapshot_dir(const std::string& work_dir, uint32_t version);

std::string wal_dir(const std::string& work_dir);

std::string runtime_dir(const std::string& work_dir);

}  // namespace gs

#endif  // GRAPHSCOPE_STORAGES_RT_MUTABLE_GRAPH_FILE_NAMES_H_