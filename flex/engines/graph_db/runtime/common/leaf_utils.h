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

#ifndef RUNTIME_COMMON_LEAF_UTILS_H_
#define RUNTIME_COMMON_LEAF_UTILS_H_

#include <boost/leaf.hpp>
#include "flex/utils/result.h"

namespace bl = boost::leaf;

#define RETURN_UNSUPPORTED_ERROR(msg) \
  return ::boost::leaf::new_error(    \
      ::gs::Status(::gs::StatusCode::UNSUPPORTED_OPERATION, msg))

#define RETURN_BAD_REQUEST_ERROR(msg) \
  return ::boost::leaf::new_error(    \
      ::gs::Status(::gs::StatusCode::BAD_REQUEST, msg))

#define RETURN_NOT_IMPLEMENTED_ERROR(msg) \
  return ::boost::leaf::new_error(        \
      ::gs::Status(::gs::StatusCode::UNIMPLEMENTED, msg))

#endif  // RUNTIME_COMMON_LEAF_UTILS_H_
