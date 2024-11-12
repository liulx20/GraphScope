/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/app/batch_update_app.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/transaction_utils.h"

namespace gs {

static void deserialize(grape::OutArchive& oarc, Any& prop) {
  PropertyType type = prop.type;
  if (type == PropertyType::Bool()) {
    oarc >> prop.value.b;
  } else if (type == PropertyType::Int32()) {
    oarc >> prop.value.i;
  } else if (type == PropertyType::UInt32()) {
    oarc >> prop.value.ui;
  } else if (type == PropertyType::Date()) {
    oarc >> prop.value.d;
  } else if (type == PropertyType::Day()) {
    oarc >> prop.value.day;
  } else if (type == PropertyType::StringView()) {
    oarc >> prop.value.s;
  } else if (type == PropertyType::Int64()) {
    oarc >> prop.value.l;
  } else if (type == PropertyType::UInt64()) {
    oarc >> prop.value.ul;
  } else if (type == PropertyType::Double()) {
    oarc >> prop.value.db;
  } else if (type == PropertyType::Float()) {
    oarc >> prop.value.f;
  } else if (type == PropertyType::Empty()) {
  } else {
    LOG(FATAL) << "Unexpected property type" << int(type.type_enum);
  }
}

bool BatchUpdateApp::Query(GraphDBSession& graph, Decoder& input,
                           Encoder& output) {
  grape::InArchive iarc;
  auto schema = db_.schema();

  iarc.AddBytes(input.data(), input.size());
  grape::OutArchive oarc(std::move(iarc));
  UpdateBatch batch;
  int8_t type;
  oarc >> type;

  if (type == 0) {
    label_t label;
    oarc >> label;
    auto pk = schema.get_vertex_primary_key(label);
    CHECK(pk.size() == 1) << "Only support single primary key";
    auto pk_type = std::get<0>(pk[0]);
    Any oid;
    oid.type = pk_type;
    deserialize(oarc, oid);
    std::vector<Any> props;
    auto properties = schema.get_vertex_properties(label);
    for (auto& prop : properties) {
      Any p;
      p.type = prop;
      deserialize(oarc, p);
      props.push_back(p);
    }
    batch.AddVertex(label, std::move(oid), std::move(props));
  } else {
    label_t src_label, dst_label, edge_label;
    Any src, dst;
    oarc >> src_label;
    auto src_pk = schema.get_vertex_primary_key(src_label);
    CHECK(src_pk.size() == 1) << "Only support single primary key";
    auto src_pk_type = std::get<0>(src_pk[0]);
    src.type = src_pk_type;
    deserialize(oarc, src);
    oarc >> dst_label;
    auto dst_pk = schema.get_vertex_primary_key(dst_label);
    CHECK(dst_pk.size() == 1) << "Only support single primary key";
    auto dst_pk_type = std::get<0>(dst_pk[0]);
    dst.type = dst_pk_type;
    deserialize(oarc, dst);
    oarc >> edge_label;
    Any value;
    auto edge_prop =
        schema.get_edge_properties(src_label, dst_label, edge_label);
    if (edge_prop.size() == 1) {
      value.type = edge_prop[0];
      deserialize(oarc, value);
    } else {
      value.type = PropertyType::Record();
      size_t size;
      oarc >> size;
      std::vector<Any> props;
      for (size_t i = 0; i < size; ++i) {
        Any p;
        p.type = edge_prop[i];
        deserialize(oarc, p);
        props.push_back(p);
      }
      value.value.record = std::move(props);
    }
    batch.AddEdge(src_label, std::move(src), dst_label, std::move(dst),
                  edge_label, std::move(value));
  }
  graph.BatchUpdate(batch);
  return true;
}
AppWrapper BatchUpdateAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new BatchUpdateApp(db), NULL);
}
}  // namespace gs