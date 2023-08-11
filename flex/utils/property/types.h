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
#include <variant>

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
};

template <typename T>
struct AnyConverter {};

template <>
struct AnyConverter<grape::EmptyType> {
  static constexpr PropertyType type = PropertyType::kEmpty;
};

template <>
struct AnyConverter<int8_t> {
  static constexpr PropertyType type = PropertyType::kInt8;
};

template <>
struct AnyConverter<uint8_t> {
  static constexpr PropertyType type = PropertyType::kUInt8;
};

template <>
struct AnyConverter<int16_t> {
  static constexpr PropertyType type = PropertyType::kInt16;
};

template <>
struct AnyConverter<uint16_t> {
  static constexpr PropertyType type = PropertyType::kUInt16;
};

template <>
struct AnyConverter<int32_t> {
  static constexpr PropertyType type = PropertyType::kInt32;
};

template <>
struct AnyConverter<uint32_t> {
  static constexpr PropertyType type = PropertyType::kUInt32;
};

template <>
struct AnyConverter<int64_t> {
  static constexpr PropertyType type = PropertyType::kInt64;
};

template <>
struct AnyConverter<uint64_t> {
  static constexpr PropertyType type = PropertyType::kUInt64;
};

struct Date {
  Date() = default;
  ~Date() = default;
  Date(const Date& rhs);
  Date(int64_t x);
  Date(const char* str);

  void reset(const char* str);
  std::string to_string() const;

  int64_t milli_second;
};

struct Any{
  static constexpr bool POD = false;
  static constexpr bool ANY = true;

  bool type;
  union {
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
    std::any any;
  };
  Any(){}
 
  template<typename T>
  Any(T&& val):type(ANY){
    new(&any) std::any(std::move(val));
  }
  template<typename T>
  Any(const T& val){
    type = ANY;
    new(&any) std::any(val);
  }

  Any(grape::EmptyType&&){
    type = POD;
    u8 = 0;
  }

  Any(const grape::EmptyType&&){
    type = POD;
    u8 = 0;
  }

  template<typename T>
  void set_val(T&& val){
    type = ANY;
    new(&any) std::any(std::move(val));
  }

  template<typename T>
  void set_val(const T& val){
    type = ANY;
    new(&any) std::any(val);
  }

  void set_val(grape::EmptyType&&){}

  void set_val(const grape::EmptyType&){}

  void set_val(uint8_t&& val){
    type = POD;
    u8 = val;
  }
  void set_val(const uint8_t& val){
    type = POD;
    u8 = val;
  }
  
  void set_val(int8_t&& val){
    type = POD;
    i8 = val;
  }
  void set_val(const int8_t& val){
    type = POD;
    i8 = val;
  }
  
  void set_val(uint16_t&& val){
    type = POD;
    u16 = val;
  }
  void set_val(const uint16_t& val){
    type = POD;
    u16 = val;
  }

  void set_val(int16_t&& val){
    type = POD;
    i16 = val;
  }
  void set_val(const int16_t& val){
    type = POD;
    i16 = val;
  }


  void set_val(uint32_t&& val){
    type = POD;
    u32 = val;
  }
  void set_val(const uint32_t& val){
    type = POD;
    u32 = val;
  }

  void set_val(int32_t&& val){
    type = POD;
    i32 = val;
  }
  void set_val(const int32_t& val){
    type = POD;
    i32 = val;
  }

  void set_val(uint64_t&& val){
    type = POD;
    u64 = val;
  }
  void set_val(const uint64_t& val){
    type = POD;
    u64 = val;
  }
  
  void set_val(int64_t&& val){
    type = POD;
    i64 = val;
  }
  void set_val(const int64_t& val){
    type = POD;
    i64 = val;
  }

  void set_val(float&& val){
    type = POD;
    f = val;
  }

  void set_val(const float& val){
    type = POD;
    f = val;
  }

  void set_val(double&& val){
    type = POD;
    db = val;
  }

  void set_val(const double& val){
    type = POD;
    db = val;
  }

  void set_val(Date&& val){
    type = POD;
    dt =std::move(val);
  }
  void set_val(const Date& val){
    type = POD;
    dt = val;
  } 

  void set_val(std::string_view&& val){
    type = POD;
    sw = std::move(val);
  }
  void set_val(const std::string_view& val){
    type = POD;
    sw = val;
  } 

  template<typename T>
  void get_val(T& data) const{
      printf("get val error\n");
      data = std::any_cast<T>(any);
  }

  void get_val(grape::EmptyType&) const{}

  void get_val(uint8_t& data) const{
    data = u8;
  }
  
  void get_val(int8_t& data) const{
    data = i8;
  }

  void get_val(uint16_t& data) const{
    data = u16;
  }
  
  void get_val(int16_t& data) const{
    data = i16;
  }

  void get_val(uint32_t& data) const{
    data = u32;
  }
  
  void get_val(int32_t& data) const{
    data = i32;
  }

  void get_val(uint64_t& data) const{
    data = u64;
  }
  
  void get_val(int64_t& data) const{
    data = i64;
  }

  void get_val(float& data) const{
    data = f;
  }
  
  void get_val(double& data) const{
    data = db;
  }

  void get_val(std::string_view& data) const{
    data = sw;
  }

  void get_val(Date& data) const{
    data = dt;
  }
  

  Any(uint8_t u8):type(POD),u8(u8){}
  Any(int8_t i8):type(POD),i8(i8){}
  Any(uint16_t u16):type(POD),u16(u16){}
  Any(int16_t i16):type(POD),i16(i16){}
  Any(uint32_t u32):type(POD),u32(u32){}
  Any(int32_t i32):type(POD),i32(i32){}
  Any(uint64_t u64):type(POD),u64(u64){}
  Any(int64_t i64):type(POD),i64(i64){}
  Any(float f):type(POD),f(f){}
  Any(double db):type(POD),db(db){}
  Any(const Date& dt):type(POD),dt(dt){}
  Any(Date&& dt):type(POD),dt(std::move(dt)){}
  Any(const std::string_view& sw):type(POD),sw(sw){}
  Any(std::string_view&& sw): sw(std::move(sw)){}
  Any(const char* s): type(ANY){
    new(&any) std::any(std::string(s));
  }
  ~Any(){
    if(type == ANY){
      any.~any();
    }
  }
  Any& operator=(const Any& a){
    if(a.type == ANY){
      type = ANY;
      new(&any) std::any(a.any);
    }else{
      memcpy(this,(void*)&a,sizeof(a));
    }
    return *this;
  }
  Any& operator=(Any&& a){
    if(a.type == ANY){
      type = ANY;
      new(&any) std::any(std::move(a.any));
    }else{
      memcpy(this,(void*)&a,sizeof(a));
    }
    return *this;
  }
  Any(const Any& a){
    if(a.type == ANY){
      type = ANY;
      new(&any) std::any(a.any);
    }else{
      memcpy(this,(void*)&a,sizeof(a));
    }
  }

  Any(Any&& a){
    if(a.type == ANY){
      type = ANY;
      new(&any) std::any(std::move(a.any));
    }else{
      memcpy(this,(void*)&a,sizeof(a));
    }
  }
};

grape::InArchive& operator<<(grape::InArchive& arc, const Date& v);
grape::OutArchive& operator>>(grape::OutArchive& arc, Date& v);

template <>
struct AnyConverter<Date> {
  static constexpr PropertyType type = PropertyType::kDate;
};

template <>
struct AnyConverter<float> {
  static constexpr PropertyType type = PropertyType::kFloat;
};

template <>
struct AnyConverter<double> {
  static constexpr PropertyType type = PropertyType::kDouble;
};

template <>
struct AnyConverter<std::string> {
  static constexpr PropertyType type = PropertyType::kString;
};

template <>
struct AnyConverter<std::string_view> {
  static constexpr PropertyType type = PropertyType::kStringView;
};

class Property;

template <>
struct AnyConverter<std::vector<Property>> {
  static constexpr PropertyType type = PropertyType::kList;
};

class Property {
 public:
  Property() : type_(PropertyType::kEmpty), value_(grape::EmptyType()) {}
  ~Property() {}

  Property(const Property& rhs) : type_(rhs.type_), value_(rhs.value_) {}

  Property(Property&& rhs) noexcept
      : type_(rhs.type_), value_(std::move(rhs.value_)) {}

  Property& operator=(const Property& rhs) {
    type_ = rhs.type_;
    value_ = rhs.value_;
    return *this;
  }

  Property& operator=(Property&& rhs) noexcept {
    type_ = rhs.type_;
    value_ = std::move(rhs.value_);
    return *this;
  }

  template <typename T>
  Property& operator=(const T& val) {
    set_value(val);
    return *this;
  }

  template <typename T>
  Property& operator=(T&& val) {
    set_value(std::move(val));
    return *this;
  }

  template <typename T, std::enable_if_t<!std::is_same_v<Property, T>, int> = 0>
  void set_value(const T& val) {
    type_ = AnyConverter<T>::type;
    value_.set_val(val);
  }

  template <typename T, std::enable_if_t<std::is_same_v<Property, T>, int> = 0>
  void set_value(const T& val) {
    type_ = val.type_;
    value_.set_val(val.value_);
  }

  template <typename T, std::enable_if_t<!std::is_same_v<Property, T>, int> = 0>
  void set_value(T&& val) {
    type_ = AnyConverter<T>::type;
    value_.set_val(std::move(val));
  }

  template <typename T, std::enable_if_t<std::is_same_v<Property, T>, int> = 0>
  void set_value(T&& val) {
    type_ = val.type_;
    value_.set_val(std::move(val.value_));
  }

  template <typename T>
  static Property From(const T& val) {
    Property ret;
    ret.set_value(val);
    return ret;
  }

  template <typename T>
  T get_value() const {
    T data;
    value_.get_val(data);
    return data;
  }

  PropertyType type() const { return type_; }

  void set_type(PropertyType type) { type_ = type; }

  bool empty() const { return type_ == PropertyType::kEmpty; }

  void clear() {
    type_ = PropertyType::kEmpty;
    value_ = Any();
  }

  void swap(Property& rhs) {
    std::swap(type_, rhs.type_);
    std::swap(value_, rhs.value_);
  }

  std::string to_string() const {
    if(type_ == PropertyType::kInt32){
      return std::to_string(get_value<int>());
    } else if (type_ == PropertyType::kInt64) {
      return std::to_string(get_value<int64_t>());
    } else if(type_ == PropertyType::kString){
      return get_value<std::string>();
    } else if (type_ == PropertyType::kStringView) {
      const auto& s = get_value<std::string_view>();
      return std::string(s.data(), s.size());
      //      return value.s.to_string();
    } else if (type_ == PropertyType::kDate) {
      return std::to_string(get_value<Date>().milli_second);
    } else if (type_ == PropertyType::kEmpty) {
      return "NULL";
    } else if (type_ == PropertyType::kDouble) {
      return std::to_string(get_value<double>());
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type_);
      return "";
    }
  }

  bool operator==(const Property& rhs) const {
    if (type_ != rhs.type_) {
      return false;
    }
    switch (type_) {
    case PropertyType::kEmpty:
      return true;
    case PropertyType::kUInt8:
      return get_value<uint8_t>() == rhs.get_value<uint8_t>();
    case PropertyType::kInt8:
      return get_value<int8_t>() == rhs.get_value<int8_t>();
    case PropertyType::kUInt16:
      return get_value<uint16_t>() == rhs.get_value<uint16_t>();
    case PropertyType::kInt16:
      return get_value<int16_t>() == rhs.get_value<int16_t>();
    case PropertyType::kUInt32:
      return get_value<uint32_t>() == rhs.get_value<uint32_t>();
    case PropertyType::kInt32:
      return get_value<int32_t>() == rhs.get_value<int32_t>();
    case PropertyType::kUInt64:
      return get_value<uint64_t>() == rhs.get_value<uint64_t>();
    case PropertyType::kInt64:
      return get_value<int64_t>() == rhs.get_value<int64_t>();
    case PropertyType::kDate:
      return get_value<Date>().milli_second ==
             rhs.get_value<Date>().milli_second;
    case PropertyType::kFloat:
      return get_value<float>() == rhs.get_value<float>();
    case PropertyType::kDouble:
      return get_value<double>() == rhs.get_value<double>();
    case PropertyType::kString:
      return get_value<std::string>() == rhs.get_value<std::string>();
    case PropertyType::kStringView:
      return get_value<std::string_view>() == rhs.get_value<std::string_view>();
    case PropertyType::kList:
      return get_value<std::vector<Property>>() ==
             rhs.get_value<std::vector<Property>>();
    default:
      return false;
    }
  }

  bool operator!=(const Property& rhs) const { return !(*this == rhs); }

 private:
  PropertyType type_;
  Any value_;
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