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

#ifndef GRAPHSCOPE_TYPES_H_
#define GRAPHSCOPE_TYPES_H_

#include <assert.h>
#include <any>

#include <istream>
#include <ostream>
#include <vector>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

enum class StorageStrategy {
  kNone,
  kMem,
};

enum class PropertyType {
  kEmpty,
  kInt8,
  kUInt8,
  kInt16,
  kUInt16,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kDate,
  kFloat,
  kDouble,
  kStringView,
  kString,
  kList,
  kAny
};

struct Date {
  Date() = default;
  ~Date() = default;
  // Date(const Date& rhs);
  Date(int64_t x);
  Date(const char* str);

  void reset(const char* str);
  std::string to_string() const;

  int64_t milli_second;
};

template <typename T>
struct AnyConverter;

struct Property {
  union AnyValue {
    AnyValue() {}
    ~AnyValue() {}

    uint8_t u8;
    int8_t i8;
    uint16_t u16;
    int16_t i16;
    uint32_t u32;
    int32_t i32;
    uint64_t u64;
    int64_t i64;
    float f;
    double db;
    Date dt;
    std::string_view sw;
    std::string s;
    std::vector<Property> vec;
    std::any any;
  };

  Property() : type_(PropertyType::kEmpty) {}
  Property(const Property& prop) : type_(prop.type_) {
    if (type_ == PropertyType::kString) {
      new (&(value.s)) std::string(prop.value.s);
    } else if (type_ == PropertyType::kList) {
      new (&(value.vec)) std::vector<Property>(prop.value.vec);
    } else if (type_ == PropertyType::kAny) {
      new (&(value.any)) std::any(prop.value.any);
    } else {
      memcpy(&value, &prop.value, sizeof(prop.value));
    }
  }

  Property(Property&& prop) : type_(prop.type_) {
    if (type_ == PropertyType::kString) {
      new (&(value.s)) std::string(std::move(prop.value.s));
    } else if (type_ == PropertyType::kList) {
      new (&(value.vec)) std::vector<Property>(std::move(prop.value.vec));
    } else if (type_ == PropertyType::kAny) {
      new (&(value.any)) std::any(std::move(prop.value.any));
    } else {
      memcpy(&value, &prop.value, sizeof(prop.value));
    }
  }

  Property& operator=(const Property& prop) {
    if (this == &prop) {
      return *this;
    }
    if (type_ == PropertyType::kString) {
      value.s.~basic_string();
    } else if (type_ == PropertyType::kList) {
      value.vec.~vector();
    } else if (type_ == PropertyType::kAny) {
      value.any.~any();
    }
    type_ = prop.type_;
    if (type_ == PropertyType::kString) {
      new (&(value.s)) std::string(prop.value.s);
    } else if (type_ == PropertyType::kList) {
      new (&(value.vec)) std::vector<Property>(prop.value.vec);
    } else if (type_ == PropertyType::kAny) {
      new (&(value.any)) std::any(prop.value.any);
    } else {
      memcpy(&value, &prop.value, sizeof(prop.value));
    }
    return *this;
  }

  Property& operator=(Property&& prop) {
    if (this == &prop) {
      return *this;
    }
    if (type_ == PropertyType::kString) {
      value.s.~basic_string();
    } else if (type_ == PropertyType::kList) {
      value.vec.~vector();
    } else {
      value.any.~any();
    }
    type_ = prop.type_;
    if (type_ == PropertyType::kString) {
      new (&(value.s)) std::string(std::move(prop.value.s));
    } else if (type_ == PropertyType::kList) {
      new (&(value.vec)) std::vector<Property>(std::move(prop.value.vec));
    } else if (type_ == PropertyType::kAny) {
      new (&(value.any)) std::any(std::move(prop.value.any));
    } else {
      memcpy(&value, &prop.value, sizeof(prop.value));
    }
    return *this;
  }

  ~Property() {
    if (type_ == PropertyType::kString) {
      value.s.~basic_string();
    } else if (type_ == PropertyType::kList) {
      value.vec.~vector();
    } else if (type_ == PropertyType::kAny) {
      value.any.~any();
    }
  }

  PropertyType type() const { return type_; }

  void set_type(PropertyType tp) { type_ = tp; }

  template <typename T>
  T get_value() const {
    T data;
    get_val(data);
    return data;
  }

  template <typename T>
  void get_val(T& data) const {
    // printf("bad type: %d\n",(int)type_);
    data = std::any_cast<T>(value.any);
  }

  void get_val(uint8_t& data) const { data = value.u8; }

  void get_val(int8_t& data) const { data = value.i8; }

  void get_val(uint16_t& data) const { data = value.u16; }

  void get_val(int16_t& data) const { data = value.i16; }

  void get_val(uint32_t& data) const { data = value.u32; }

  void get_val(int32_t& data) const { data = value.i32; }

  void get_val(uint64_t& data) const { data = value.u64; }

  void get_val(int64_t& data) const { data = value.i64; }

  void get_val(double& data) const { data = value.db; }

  void get_val(Date& d) const { d.milli_second = value.dt.milli_second; }

  void get_val(std::string_view& sw) const { sw = value.sw; }

  void get_val(float& f) const { f = value.f; }

  void get_val(grape::EmptyType&) const {}

  void get_val(std::string& s) const { s = value.s; }

  void get_val(std::vector<Property>& vec) const { vec = value.vec; }

  template <typename T>
  void set_value(T&& val) {
    set_val(std::forward<T>(val));
  }

  template <typename T>
  void set_val(const T&& val) {
    type_ = PropertyType::kAny;
    new (&(value.any)) std::any(std::move(val));
  }

  template <typename T>
  void set_val(const T& val) {
    type_ = PropertyType::kAny;
    printf("set error %d\n", (int) type_);
    new (&(value.any)) std::any(val);
  }

  void set_val(const std::string& s) {
    type_ = PropertyType::kString;
    new (&(value.s)) std::string(s);
  }

  void set_val(std::string&& s) {
    type_ = PropertyType::kString;
    new (&(value.s)) std::string(std::move(s));
  }

  void set_val(const std::vector<Property>& vec) {
    type_ = PropertyType::kList;
    new (&(value.vec)) std::vector<Property>(vec);
  }

  void set_val(std::vector<Property>&& vec) {
    type_ = PropertyType::kList;
    new (&(value.vec)) std::vector<Property>(std::move(vec));
  }

  void set_val(const grape::EmptyType&) {}

  void set_val(grape::EmptyType&&) {}

  void set_val(const uint8_t& u8) {
    type_ = PropertyType::kUInt8;
    value.u8 = u8;
  }

  void set_val(uint8_t&& u8) {
    type_ = PropertyType::kUInt8;
    value.u8 = u8;
  }

  void set_val(const int8_t& i8) {
    type_ = PropertyType::kInt8;
    value.i8 = i8;
  }

  void set_val(int8_t&& i8) {
    type_ = PropertyType::kInt8;
    value.i8 = i8;
  }

  void set_val(const uint16_t& u16) {
    type_ = PropertyType::kUInt16;
    value.u16 = u16;
  }

  void set_val(uint16_t&& u16) {
    type_ = PropertyType::kUInt16;
    value.u16 = u16;
  }

  void set_val(const int16_t& i16) {
    type_ = PropertyType::kInt16;
    value.i16 = i16;
  }

  void set_val(int16_t&& i16) {
    type_ = PropertyType::kInt16;
    value.i16 = i16;
  }

  void set_val(const uint32_t& v) {
    type_ = PropertyType::kUInt32;
    value.u32 = v;
  }

  void set_val(uint32_t&& v) {
    type_ = PropertyType::kUInt32;
    value.u32 = v;
  }

  void set_val(const int32_t& v) {
    type_ = PropertyType::kInt32;
    value.i32 = v;
  }

  void set_val(int32_t&& v) {
    type_ = PropertyType::kInt32;
    value.i32 = v;
  }

  void set_val(const uint64_t& v) {
    type_ = PropertyType::kUInt64;
    value.u64 = v;
  }

  void set_val(uint64_t&& v) {
    type_ = PropertyType::kUInt64;
    value.u64 = v;
  }

  void set_val(const int64_t& v) {
    type_ = PropertyType::kInt64;
    value.i64 = v;
  }

  void set_val(int64_t&& v) {
    type_ = PropertyType::kInt64;
    value.i64 = v;
  }

  void set_val(const Date& v) {
    type_ = PropertyType::kDate;
    value.dt = v;
  }

  void set_val(Date&& v) {
    type_ = PropertyType::kDate;
    value.dt = v;
  }

  void set_val(const std::string_view& v) {
    type_ = PropertyType::kStringView;
    value.sw = v;
  }

  void set_val(std::string_view&& v) {
    type_ = PropertyType::kStringView;
    value.sw = v;
  }

  void set_val(const double& db) {
    type_ = PropertyType::kDouble;
    value.db = db;
  }

  void set_val(double&& db) {
    type_ = PropertyType::kDouble;
    value.db = db;
  }

  void set_val(const float& f) {
    type_ = PropertyType::kFloat;
    value.f = f;
  }

  void set_val(float&& f) {
    type_ = PropertyType::kFloat;
    value.f = f;
  }

  std::string to_string() const {
    if (type_ == PropertyType::kUInt8) {
      return std::to_string(value.u8);
    } else if (type_ == PropertyType::kInt8) {
      return std::to_string(value.i8);
    } else if (type_ == PropertyType::kUInt16) {
      return std::to_string(value.u16);
    } else if (type_ == PropertyType::kInt16) {
      return std::to_string(value.i16);
    } else if (type_ == PropertyType::kInt32) {
      return std::to_string(value.i32);
    } else if (type_ == PropertyType::kUInt32) {
      return std::to_string(value.u32);
    } else if (type_ == PropertyType::kUInt64) {
      return std::to_string(value.u64);
    } else if (type_ == PropertyType::kInt64) {
      return std::to_string(value.i64);
    } else if (type_ == PropertyType::kStringView) {
      return std::string(value.sw.data(), value.sw.size());
      //      return value.s.to_string();
    } else if (type_ == PropertyType::kString) {
      return value.s;
    } else if (type_ == PropertyType::kDate) {
      return value.dt.to_string();
    } else if (type_ == PropertyType::kEmpty) {
      return "NULL";
    } else if (type_ == PropertyType::kDouble) {
      return std::to_string(value.db);
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type_);
      return "";
    }
  }

  template <typename T>
  static Property From(T&& value) {
    Property prop;
    prop.set_value(std::forward<T>(value));
    return prop;
    // return AnyConverter<T>::to_any(value);
  }

  PropertyType type_;
  AnyValue value;
};

template <typename T>
struct AnyConverter {};

template <>
struct AnyConverter<int8_t> {
  static constexpr PropertyType type = PropertyType::kInt8;

  static Property to_any(const int8_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.i8 = value;
    return ret;
  }
};

template <>
struct AnyConverter<uint8_t> {
  static constexpr PropertyType type = PropertyType::kUInt8;

  static Property to_any(const uint8_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.u8 = value;
    return ret;
  }
};

template <>
struct AnyConverter<int16_t> {
  static constexpr PropertyType type = PropertyType::kInt16;

  static Property to_any(const int16_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.i16 = value;
    return ret;
  }
};

template <>
struct AnyConverter<uint16_t> {
  static constexpr PropertyType type = PropertyType::kUInt16;

  static Property to_any(const uint16_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.u16 = value;
    return ret;
  }
};

template <>
struct AnyConverter<int32_t> {
  static constexpr PropertyType type = PropertyType::kInt32;

  static Property to_any(const int32_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.i32 = value;
    return ret;
  }
};

template <>
struct AnyConverter<uint32_t> {
  static constexpr PropertyType type = PropertyType::kUInt32;

  static Property to_any(const uint32_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.u32 = value;
    return ret;
  }
};

template <>
struct AnyConverter<std::string> {
  static constexpr PropertyType type = PropertyType::kString;

  static Property to_any(const std::string& value) {
    Property ret;
    ret.set_val(value);
    return ret;
  }
};

template <>
struct AnyConverter<uint64_t> {
  static constexpr PropertyType type = PropertyType::kUInt64;

  static Property to_any(const uint64_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.u64 = value;
    return ret;
  }
};

template <>
struct AnyConverter<int64_t> {
  static constexpr PropertyType type = PropertyType::kInt64;

  static Property to_any(const int64_t& value) {
    Property ret;
    ret.type_ = type;
    ret.value.i64 = value;
    return ret;
  }
};

grape::InArchive& operator<<(grape::InArchive& arc, const Date& v);
grape::OutArchive& operator>>(grape::OutArchive& arc, Date& v);

template <>
struct AnyConverter<Date> {
  static constexpr PropertyType type = PropertyType::kDate;

  static Property to_any(const Date& value) {
    Property ret;
    ret.type_ = type;
    ret.value.dt.milli_second = value.milli_second;
    return ret;
  }
};

template <>
struct AnyConverter<std::string_view> {
  static constexpr PropertyType type = PropertyType::kStringView;

  static Property to_any(const std::string_view& value) {
    Property ret;
    ret.type_ = type;
    ret.value.sw = value;
    return ret;
  }
};

template <>
struct AnyConverter<std::vector<Property>> {
  static constexpr PropertyType type = PropertyType::kList;
  static Property to_any(const std::vector<Property>& value) {
    Property ret;
    ret.set_val(value);
    return ret;
  }
};

template <>
struct AnyConverter<grape::EmptyType> {
  static constexpr PropertyType type = PropertyType::kEmpty;

  static Property to_any(const grape::EmptyType& value) { return Property(); }
};

template <>
struct AnyConverter<double> {
  static constexpr PropertyType type = PropertyType::kDouble;

  static Property to_any(const double& value) {
    Property ret;
    ret.type_ = PropertyType::kDouble;
    ret.value.db = value;
    return ret;
  }
};

template <>
struct AnyConverter<float> {
  static constexpr PropertyType type = PropertyType::kFloat;

  static Property to_any(const float& value) {
    Property ret;
    ret.type_ = PropertyType::kFloat;
    ret.value.f = value;
    return ret;
  }
};

void ParseRecord(const char* line, std::vector<Property>& rec);

void ParseRecord(const char* line, int64_t& id, std::vector<Property>& rec);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int64_t& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, double& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, uint32_t& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::vector<Property>& rec);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::string_view& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::string& prop);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& value);

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, gs::PropertyType pt) {
  switch (pt) {
  case gs::PropertyType::kEmpty:
    os << "empty";
    break;
  case gs::PropertyType::kUInt8:
    os << "uint8";
    break;
  case gs::PropertyType::kInt8:
    os << "int8";
    break;
  case gs::PropertyType::kUInt16:
    os << "uint16";
    break;
  case gs::PropertyType::kInt16:
    os << "int16";
    break;
  case gs::PropertyType::kUInt32:
    os << "uint32";
    break;
  case gs::PropertyType::kInt32:
    os << "int32";
    break;
  case gs::PropertyType::kUInt64:
    os << "uint64";
    break;
  case gs::PropertyType::kInt64:
    os << "int64";
    break;
  case gs::PropertyType::kDate:
    os << "Date";
    break;
  case gs::PropertyType::kFloat:
    os << "float";
    break;
  case gs::PropertyType::kDouble:
    os << "double";
    break;
  case gs::PropertyType::kString:
    os << "string";
    break;
  case gs::PropertyType::kStringView:
    os << "string_view";
    break;
  case gs::PropertyType::kList:
    os << "list";
    break;
  default:
    os << "Unrecoginzed type";
    break;
  }
  return os;
}

}  // namespace std

#endif  // GRAPHSCOPE_TYPES_H_