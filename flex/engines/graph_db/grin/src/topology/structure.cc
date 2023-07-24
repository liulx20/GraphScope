/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "grin/src/predefine.h"

#include "grin/include/topology/structure.h"

/**
 * @brief Get a (non-partitioned) graph from storage
 * @param uri The URI of the graph.
 * Current URI format:
 * flex://{path_to_yaml}
 * @return A graph handle.
 */
GRIN_GRAPH grin_get_graph_from_storage(const char* uri) {
  std::string _uri(uri);
  std::string::size_type pos = _uri.find("://");
  if (pos == std::string::npos) {
    return GRIN_NULL_GRAPH;
  }
  auto protocol = _uri.substr(0, pos);
  if (protocol != "flex") {
    return GRIN_NULL_GRAPH;
  }
  _uri = _uri.substr(pos + 3);
  std::string graph_schema_path = _uri + "/modern_graph.yaml";
  std::string data_path = uri;
  std::string bulk_load_config_path = _uri + "/bulk_load.yaml";
  if (!std::filesystem::exists(graph_schema_path) ||
      !(std::filesystem::exists(bulk_load_config_path))) {
    return GRIN_NULL_GRAPH;
  }
  auto ret = gs::Schema::LoadFromYaml(graph_schema_path, bulk_load_config_path);
  const auto& schema = std::get<0>(ret);
  auto& vertex_files = std::get<1>(ret);

  auto& edge_files = std::get<2>(ret);

  GRIN_GRAPH_T* g = new GRIN_GRAPH_T();
  g->g.Init(schema, vertex_files, edge_files);
  init_cache(g);
  return g;
}

void grin_destroy_graph(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  delete _g;
}

// Graph
#if defined(GRIN_ASSUME_HAS_DIRECTED_GRAPH) && \
    defined(GRIN_ASSUME_HAS_UNDIRECTED_GRAPH)
bool grin_is_directed(GRIN_GRAPH g) { return true; }
#endif

#ifdef GRIN_ASSUME_HAS_MULTI_EDGE_GRAPH
bool grin_is_multigraph(GRIN_GRAPH) { return true; }
#endif

#ifndef GRIN_WITH_VERTEX_PROPERTY
size_t grin_get_vertex_num(GRIN_GRAPH);
#endif

#ifndef GRIN_WITH_EDGE_PROPERTY
size_t grin_get_edge_num(GRIN_GRAPH);
#endif

// Vertex
void grin_destroy_vertex(GRIN_GRAPH, GRIN_VERTEX v) {}

bool grin_equal_vertex(GRIN_GRAPH g, GRIN_VERTEX v1, GRIN_VERTEX v2) {
  return v1 == v2;
}

// Data
#ifdef GRIN_WITH_VERTEX_DATA
GRIN_DATATYPE grin_get_vertex_data_datatype(GRIN_GRAPH, GRIN_VERTEX);

const void* grin_get_vertex_data_value(GRIN_GRAPH, GRIN_VERTEX);
#endif

// Edge
void grin_destroy_edge(GRIN_GRAPH, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  delete _e;
}

GRIN_VERTEX grin_get_src_vertex_from_edge(GRIN_GRAPH, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto v = _e->src;
  return v;
}

GRIN_VERTEX grin_get_dst_vertex_from_edge(GRIN_GRAPH, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto v = _e->dst;
  return v;
}

#ifdef GRIN_WITH_EDGE_DATA
GRIN_DATATYPE grin_get_edge_data_datatype(GRIN_GRAPH, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto type = _e->data.type();
  return _get_data_type(type);
}

const void* grin_get_edge_data_value(GRIN_GRAPH, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto type = _e->data.type();
  switch (_get_data_type(type)) {
  case GRIN_DATATYPE::Int32: {
    return new int32_t(_e->data.get_value<int>());
  }
  case GRIN_DATATYPE::Int64: {
    return new int64_t(_e->data.get_value<int64_t>());
  }
  case GRIN_DATATYPE::Double: {
    return new double(_e->data.get_value<double>());
  }
  case GRIN_DATATYPE::String: {
    auto s = _e->data.get_value<std::string_view>();
    auto len = s.size() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.data());
    return out;
  }
  case GRIN_DATATYPE::Timestamp64: {
    return new int64_t(_e->data.get_value<gs::Date>().milli_second);
  }
  default:
    return GRIN_NULL_EDGE_DATA;
  }
}
#endif