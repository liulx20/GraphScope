#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include "flex/planner/graph_planner.h"

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()
#include <zlib.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace gs {

std::vector<unsigned char> base64_decode(const std::string& in) {
  // Base64 decode implementation here
  // You can use a library or implement your own for simplicity

  // This is a simple base64 decode function, ensure to replace it.
  // In production code, you would want to include error-checking.

  static const std::string base64_chars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz"
      "0123456789+/";

  size_t in_len = in.size();
  std::vector<unsigned char> buffer;

  for (size_t i = 0; i < in_len;) {
    uint32_t val = 0;
    for (int j = 0; j < 4; ++j) {
      if (i < in_len && in[i] != '=') {
        val = (val << 6) + base64_chars.find(in[i]);
      } else {
        val <<= 6;
      }
      i++;
    }
    buffer.push_back((val >> 16) & 0xFF);
    if (i > 2) {
      buffer.push_back((val >> 8) & 0xFF);
    }
    if (i > 3) {
      buffer.push_back(val & 0xFF);
    }
  }

  return buffer;
}

std::string decompress(const std::string& compressed) {
  unsigned char buffer[1024];
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = compressed.size();  // 输入字节数
  auto vec = base64_decode(compressed);
  strm.next_in = vec.data();  // 输入数据

  // 初始化解压缩
  if (inflateInit(&strm) != Z_OK)
    return "";

  std::string result;

  // 解压缩数据
  do {
    strm.avail_out = sizeof(buffer);
    strm.next_out = buffer;
    int ret = inflate(&strm, Z_NO_FLUSH);

    if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
      inflateEnd(&strm);
      return "";
    }

    result.append(reinterpret_cast<char*>(buffer),
                  sizeof(buffer) - strm.avail_out);
  } while (strm.avail_out == 0);

  inflateEnd(&strm);
  return result;
}

std::string generate_compiler_config(const std::string& schema,
                                     const std::string& statistics,
                                     const std::vector<std::string>& rules) {
  std::stringstream ss;
  std::string configs =
      "compiler:\n"
      "  planner:\n"
      "    is_on: true\n"
      "    opt: CBO\n"
      "    rules:\n";
  for (const auto& rule : rules) {
    ss << "      - " << rule << "\n";
  }
  std::string rules_str = ss.str();
  configs += rules_str;
  configs +=
      "    trim_class_names: GraphLogicalExpand\n"
      "    join_min_pattern_size: 15\n"
      "  meta:\n"
      "    reader:\n"
      "      schema:\n";
  configs += "        uri: " + schema + "\n";
  configs += "        interval: 1000 # ms\n";

  configs += "      statistics:\n";

  configs += "        uri: " + statistics + "\n";
  configs += "        interval: 86400000 # ms\n";
  configs +=
      "  endpoint:\n"
      "    default_listen_address: localhost\n"
      "  query_timeout: 40000\n"
      "  physical.opt.config: proto\n";
  return configs;
}

void generate_compiler_configs(const std::string& graph_yaml,
                               const std::string& statistics_json,
                               const std::string& path) {
  std::vector<std::string> rules = {
      "FilterIntoJoinRule",  "FilterMatchRule",      "NotMatchToAntiJoinRule",
      "ExtendIntersectRule", "ExpandGetVFusionRule", "FlatJoinToExpandRule",
      "FieldTrimRule"};
  std::string compiler_config =
      generate_compiler_config(graph_yaml, statistics_json, rules);
  std::ofstream out(path);
  out << compiler_config;
  out.close();
}

bool generate_plan(
    const std::string& query, const std::string& statistics,
    const std::string& compiler_yaml,
    std::unordered_map<std::string, physical::PhysicalPlan>& plan_cache) {
  // dump query to file
  const char* graphscope_dir = getenv("GRAPHSCOPE_DIR");
  if (graphscope_dir == nullptr) {
    std::cerr << "GRAPHSCOPE_DIR is not set!" << std::endl;
    graphscope_dir = "../../../GraphScope/";
  }

  auto id = std::this_thread::get_id();

  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();

  const std::string compiler_config_path =
      "/tmp/compiler_config_" + thread_id + ".yaml";
  // const std::string query_file = "/tmp/temp" + thread_id + ".cypher";
  // const std::string output_file = "/tmp/temp" + thread_id + ".pb";
  const std::string jar_path = std::string(graphscope_dir) +
                               "/interactive_engine/compiler/target/"
                               "compiler-0.0.1-SNAPSHOT.jar:" +
                               std::string(graphscope_dir) +
                               "/interactive_engine/compiler/target/libs/";
  const std::string djna_path =
      std::string(graphscope_dir) +
      "/interactive_engine/executor/ir/target/release/";
  const std::string schema_path = compiler_yaml;
  auto raw_query = query;  // decompress(query);

  generate_compiler_configs(compiler_yaml, statistics, compiler_config_path);

  // call compiler to generate plan
  {
    // pid_t pid = fork();

    gs::jni::GraphPlannerWrapper planner(jar_path, djna_path, compiler_yaml,
                                         statistics);
    if (!planner.is_valid()) {
      LOG(ERROR) << "Invalid GraphPlannerWrapper.";
      return false;
    }
    auto plan = planner.CompilePlan(compiler_config_path, raw_query);

    plan_cache[query] = plan;
    // clean up temp files
    { unlink(compiler_config_path.c_str()); }
  }

  return true;
}

void parse_params(std::string_view sw,
                  std::map<std::string, std::string>& params) {
  std::string key, value;
  size_t i = 0;
  while (i < sw.size()) {
    size_t begin = i;
    for (; i < sw.size(); ++i) {
      if (sw[i] == '=') {
        key = std::string(sw.substr(begin, i - begin));
        break;
      }
    }
    begin = ++i;
    for (; i < sw.size(); ++i) {
      if (i + 1 < sw.size() && sw[i] == '&' && sw[i + 1] == '?') {
        value = std::string(sw.substr(begin, i - begin));
        ++i;
        break;
      }
    }
    if (i == sw.size()) {
      value = std::string(sw.substr(begin, i - begin));
    }
    i++;
    params[key] = value;
  }
}

}  // namespace gs