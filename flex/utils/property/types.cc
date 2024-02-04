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

#include "flex/utils/property/types.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

const PropertyType PropertyType::kEmpty =
    PropertyType(impl::PropertyTypeImpl::kEmpty);
const PropertyType PropertyType::kBool =
    PropertyType(impl::PropertyTypeImpl::kBool);
const PropertyType PropertyType::kUInt8 =
    PropertyType(impl::PropertyTypeImpl::kUInt8);
const PropertyType PropertyType::kUInt16 =
    PropertyType(impl::PropertyTypeImpl::kUInt16);
const PropertyType PropertyType::kInt32 =
    PropertyType(impl::PropertyTypeImpl::kInt32);
const PropertyType PropertyType::kUInt32 =
    PropertyType(impl::PropertyTypeImpl::kUInt32);
const PropertyType PropertyType::kFloat =
    PropertyType(impl::PropertyTypeImpl::kFloat);
const PropertyType PropertyType::kInt64 =
    PropertyType(impl::PropertyTypeImpl::kInt64);
const PropertyType PropertyType::kUInt64 =
    PropertyType(impl::PropertyTypeImpl::kUInt64);
const PropertyType PropertyType::kDouble =
    PropertyType(impl::PropertyTypeImpl::kDouble);
const PropertyType PropertyType::kDate =
    PropertyType(impl::PropertyTypeImpl::kDate);
const PropertyType PropertyType::kDay =
    PropertyType(impl::PropertyTypeImpl::kDay);
const PropertyType PropertyType::kString =
    PropertyType(impl::PropertyTypeImpl::kString);
const PropertyType PropertyType::kStringMap =
    PropertyType(impl::PropertyTypeImpl::kStringMap);
const PropertyType PropertyType::kCharArray4 =
    PropertyType(impl::PropertyTypeImpl::kCharArray4);
const PropertyType PropertyType::kCharArray8 =
    PropertyType(impl::PropertyTypeImpl::kCharArray8);
const PropertyType PropertyType::kCharArray12 =
    PropertyType(impl::PropertyTypeImpl::kCharArray12);
const PropertyType PropertyType::kCharArray16 =
    PropertyType(impl::PropertyTypeImpl::kCharArray16);
const PropertyType PropertyType::kCharArray20 =
    PropertyType(impl::PropertyTypeImpl::kCharArray20);
const PropertyType PropertyType::kCharArray24 =
    PropertyType(impl::PropertyTypeImpl::kCharArray24);

bool PropertyType::operator==(const PropertyType& other) const {
  if (type_enum == impl::PropertyTypeImpl::kVarChar &&
      other.type_enum == impl::PropertyTypeImpl::kVarChar) {
    return additional_type_info.max_length ==
           other.additional_type_info.max_length;
  }
  if ((type_enum == impl::PropertyTypeImpl::kString &&
       other.type_enum == impl::PropertyTypeImpl::kVarChar) ||
      (type_enum == impl::PropertyTypeImpl::kVarChar &&
       other.type_enum == impl::PropertyTypeImpl::kString)) {
    return true;
  }
  return type_enum == other.type_enum;
}

bool PropertyType::operator!=(const PropertyType& other) const {
  return !(*this == other);
}

bool PropertyType::IsVarchar() const {
  return type_enum == impl::PropertyTypeImpl::kVarChar;
}

uint16_t PropertyType::NumBytes() const {
  switch (type_enum) {
  case impl::PropertyTypeImpl::kEmpty:
    return 0;
  case impl::PropertyTypeImpl::kBool:
    return sizeof(bool);
  case impl::PropertyTypeImpl::kUInt8:
    return sizeof(uint8_t);
  case impl::PropertyTypeImpl::kUInt16:
    return sizeof(uint16_t);
  case impl::PropertyTypeImpl::kInt32:
    return sizeof(int32_t);
  case impl::PropertyTypeImpl::kUInt32:
    return sizeof(uint32_t);
  case impl::PropertyTypeImpl::kFloat:
    return sizeof(float);
  case impl::PropertyTypeImpl::kInt64:
    return sizeof(int64_t);
  case impl::PropertyTypeImpl::kUInt64:
    return sizeof(uint64_t);
  case impl::PropertyTypeImpl::kDouble:
    return sizeof(double);
  case impl::PropertyTypeImpl::kCharArray4:
    return 4;
  case impl::PropertyTypeImpl::kCharArray8:
    return 8;
  case impl::PropertyTypeImpl::kCharArray12:
    return 12;
  case impl::PropertyTypeImpl::kCharArray16:
    return 16;
  case impl::PropertyTypeImpl::kCharArray20:
    return 20;
  case impl::PropertyTypeImpl::kCharArray24:
    return 24;
  default:
    LOG(FATAL) << "Can not get num bytes for type: " << (*this);
  }
}

/////////////////////////////// Get Type Instance
//////////////////////////////////
PropertyType PropertyType::Empty() {
  return PropertyType(impl::PropertyTypeImpl::kEmpty);
}
PropertyType PropertyType::Bool() {
  return PropertyType(impl::PropertyTypeImpl::kBool);
}
PropertyType PropertyType::UInt8() {
  return PropertyType(impl::PropertyTypeImpl::kUInt8);
}
PropertyType PropertyType::UInt16() {
  return PropertyType(impl::PropertyTypeImpl::kUInt16);
}
PropertyType PropertyType::Int32() {
  return PropertyType(impl::PropertyTypeImpl::kInt32);
}
PropertyType PropertyType::UInt32() {
  return PropertyType(impl::PropertyTypeImpl::kUInt32);
}
PropertyType PropertyType::Float() {
  return PropertyType(impl::PropertyTypeImpl::kFloat);
}
PropertyType PropertyType::Int64() {
  return PropertyType(impl::PropertyTypeImpl::kInt64);
}
PropertyType PropertyType::UInt64() {
  return PropertyType(impl::PropertyTypeImpl::kUInt64);
}
PropertyType PropertyType::Double() {
  return PropertyType(impl::PropertyTypeImpl::kDouble);
}
PropertyType PropertyType::Date() {
  return PropertyType(impl::PropertyTypeImpl::kDate);
}
PropertyType PropertyType::Day() {
  return PropertyType(impl::PropertyTypeImpl::kDay);
}
PropertyType PropertyType::String() {
  return PropertyType(impl::PropertyTypeImpl::kString);
}
PropertyType PropertyType::StringMap() {
  return PropertyType(impl::PropertyTypeImpl::kStringMap);
}
PropertyType PropertyType::Varchar(uint16_t max_length) {
  return PropertyType(impl::PropertyTypeImpl::kVarChar, max_length);
}

PropertyType PropertyType::CharArray4() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray4);
}

PropertyType PropertyType::CharArray8() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray8);
}

PropertyType PropertyType::CharArray12() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray12);
}

PropertyType PropertyType::CharArray16() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray16);
}

PropertyType PropertyType::CharArray20() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray20);
}

PropertyType PropertyType::CharArray24() {
  return PropertyType(impl::PropertyTypeImpl::kCharArray24);
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const PropertyType& value) {
  in_archive << value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    in_archive << value.additional_type_info.max_length;
  }
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              PropertyType& value) {
  out_archive >> value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    out_archive >> value.additional_type_info.max_length;
  }
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value) {
  if (value.type == PropertyType::Empty()) {
    in_archive << value.type;
  } else if (value.type == PropertyType::Bool()) {
    in_archive << value.type << value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    in_archive << value.type << value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    in_archive << value.type << value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    in_archive << value.type << value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    in_archive << value.type << value.value.ui;
  } else if (value.type == PropertyType::Float()) {
    in_archive << value.type << value.value.f;
  } else if (value.type == PropertyType::Int64()) {
    in_archive << value.type << value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    in_archive << value.type << value.value.ul;
  } else if (value.type == PropertyType::Double()) {
    in_archive << value.type << value.value.db;
  } else if (value.type == PropertyType::Date()) {
    in_archive << value.type << value.value.d.milli_second;
  } else if (value.type == PropertyType::Day()) {
    in_archive << value.type << value.value.day.to_u32();
  } else if (value.type == PropertyType::String()) {
    in_archive << value.type << value.value.s;
  } else if (value.type == PropertyType::kCharArray4) {
    in_archive << value.type << std::string(value.value.array, 4);
  } else if (value.type == PropertyType::kCharArray8) {
    in_archive << value.type << std::string(value.value.array, 8);
  } else if (value.type == PropertyType::kCharArray12) {
    in_archive << value.type << std::string(value.value.array, 12);
  } else if (value.type == PropertyType::kCharArray16) {
    in_archive << value.type << std::string(value.value.array, 16);
  } else if (value.type == PropertyType::kCharArray20) {
    in_archive << value.type << std::string(value.value.array, 20);
  } else if (value.type == PropertyType::kCharArray24) {
    in_archive << value.type << std::string(value.value.array, 24);
  } else {
    in_archive << PropertyType::kEmpty;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  if (value.type == PropertyType::Empty()) {
  } else if (value.type == PropertyType::Bool()) {
    out_archive >> value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    out_archive >> value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    out_archive >> value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    out_archive >> value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    out_archive >> value.value.ui;
  } else if (value.type == PropertyType::Float()) {
    out_archive >> value.value.f;
  } else if (value.type == PropertyType::Int64()) {
    out_archive >> value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    out_archive >> value.value.ul;
  } else if (value.type == PropertyType::Double()) {
    out_archive >> value.value.db;
  } else if (value.type == PropertyType::Date()) {
    int64_t date_val;
    out_archive >> date_val;
    value.value.d.milli_second = date_val;
  } else if (value.type == PropertyType::Day()) {
    uint32_t val;
    out_archive >> val;
    value.value.day.from_u32(val);
  } else if (value.type == PropertyType::String()) {
    out_archive >> value.value.s;
  } else if (value.type == PropertyType::kCharArray4) {
    auto ptr = out_archive.GetBytes(4);
    memcpy(value.value.array, ptr, 4);
  } else if (value.type == PropertyType::kCharArray8) {
    auto ptr = out_archive.GetBytes(8);
    memcpy(value.value.array, ptr, 8);
  } else if (value.type == PropertyType::kCharArray12) {
    auto ptr = out_archive.GetBytes(12);
    memcpy(value.value.array, ptr, 12);
  } else if (value.type == PropertyType::kCharArray16) {
    auto ptr = out_archive.GetBytes(16);
    memcpy(value.value.array, ptr, 16);
  } else if (value.type == PropertyType::kCharArray20) {
    auto ptr = out_archive.GetBytes(20);
    memcpy(value.value.array, ptr, 20);
  } else if (value.type == PropertyType::kCharArray24) {
    auto ptr = out_archive.GetBytes(24);
    memcpy(value.value.array, ptr, 24);
  } else {
    value.type = PropertyType::kEmpty;
  }

  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& str) {
  in_archive << str.length();
  in_archive.AddBytes(str.data(), str.length());
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& str) {
  size_t size;
  out_archive >> size;
  str = std::string_view(reinterpret_cast<char*>(out_archive.GetBytes(size)),
                         size);
  return out_archive;
}

Date::Date(int64_t x) : milli_second(x) {}

std::string Date::to_string() const { return std::to_string(milli_second); }

Day::Day(int64_t ts) { from_timestamp(ts); }

std::string Day::to_string() const {
  return std::to_string(static_cast<int>(year())) + "-" +
         std::to_string(static_cast<int>(month())) + "-" +
         std::to_string(static_cast<int>(day()));
}

uint32_t Day::to_u32() const { return value.integer; }

void Day::from_u32(uint32_t val) { value.integer = val; }

int Day::year() const { return value.internal.year; }

int Day::month() const { return value.internal.month; }

int Day::day() const { return value.internal.day; }

int Day::hour() const { return value.internal.hour; }

}  // namespace gs
