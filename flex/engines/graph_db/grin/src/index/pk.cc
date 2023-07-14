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

#include "grin/include/include/index/pk.h"

#if defined(GRIN_ENABLE_VERTEX_PK_INDEX) && \
    defined(GRIN_ENABLE_VERTEX_PRIMARY_KEYS)
/**
 * @brief Get the vertex by primary keys row.
 * The values in the row must be in the same order as the primary keys
 * properties, which can be obtained by
 * ``grin_get_primary_keys_by_vertex_type``.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @param GRIN_ROW The values row of primary keys properties.
 * @return The vertex.
 */
GRIN_VERTEX grin_get_vertex_by_primary_keys_row(GRIN_GRAPH g,
                                                GRIN_VERTEX_TYPE label,
                                                GRIN_ROW r) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto oid = *static_cast<const gs::oid_t*>((*_r)[0]);
  uint32_t vid;

  if (!_g->get_lid(label, oid, vid)) {
    return GRIN_NULL_VERTEX;
  }
  uint64_t v = ((label * 1ull) << 32) + vid;
  return v;
}
#endif