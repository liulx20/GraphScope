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

#ifndef GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
#define GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_

#include "flex/utils/property/types.h"
#include "glog/logging.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

inline void serialize_field(grape::InArchive& arc, const gs::Property& prop) {
  arc << prop;
}

inline void deserialize_field(grape::OutArchive& arc, gs::Property& prop) {
  arc >> prop;
}

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
