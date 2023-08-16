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

inline void ParseInt32(const std::string_view& str, int& val) {
  sscanf(str.data(), "%d", &val);
}

inline void ParseInt64(const std::string_view& str, int64_t& val) {
#ifdef __APPLE__
  sscanf(str.data(), "%lld", &val);
#else
  sscanf(str.data(), "%" SCNd64, &val);
#endif
}

inline void ParseDouble(const std::string_view& str, double& val) {
  sscanf(str.data(), "%lf", &val);
}

inline void ParseUInt32(const std::string_view& str, uint32_t& val) {
#ifdef __APPLE__
  sscanf(str.data(), "%u", &val);
#else
  sscanf(str.data(), "%" SCNu32, &val);
#endif
}

inline void ParseDate(const std::string_view& str, Date& date) {
  date.reset(str.data());
}

inline void ParseString(const std::string_view& str, std::string_view& val) {
  val = str;
}

void ParseRecord(const char* line, std::vector<Property>& rec) {
  const char* cur = line;
  for (auto& item : rec) {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    if (item.type_ == PropertyType::kInt32) {
      int val;
      ParseInt32(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kInt64) {
      int64_t val;
      ParseInt64(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kDouble) {
      double val;
      ParseDouble(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kUInt32) {
      uint32_t val;
      ParseUInt32(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kDate) {
      Date val;
      ParseDate(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kStringView) {
      std::string_view val;
      ParseString(sv, val);
      item.set_value(val);
    } else if (item.type_ == PropertyType::kString) {
      std::string val(sv);
      item.set_value(val);
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(item.type_)
                 << ".";
    }
    cur = ptr + 1;
  }
}

void ParseRecord(const char* line, int64_t& id, std::vector<Property>& rec) {
  const char* cur = line;
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    ParseInt64(sv, id);
    cur = ptr + 1;
  }
  ParseRecord(cur, rec);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, uint8_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%u", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNu8, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int8_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%d", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNd8, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  uint16_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%u", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNu16, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int16_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%d", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNd16, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  uint32_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%u", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNu32, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%d", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%d", &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int64_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%lld", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNd64, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  uint64_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%llu", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNu64, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld", &src, &dst);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "", &src, &dst);
#endif
  const char* ptr = strrchr(line, '|') + 1;
  prop.reset(ptr);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld", &src, &dst);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "", &src, &dst);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, float& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%f", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%f", &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, double& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%lf", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%lf", &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::vector<Property>& rec) {
  const char* cur = line;
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view src_sv(cur, ptr - cur);
    ParseInt64(src_sv, src);
    cur = ptr + 1;

    ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view dst_sv(cur, ptr - cur);
    ParseInt64(dst_sv, dst);
    cur = ptr + 1;
  }
  ParseRecord(cur, rec);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::string_view& prop) {
  const char* cur = line;
  const char* ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  std::string_view src_sv(cur, ptr - cur);
  ParseInt64(src_sv, src);
  cur = ptr + 1;

  ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  std::string_view dst_sv(cur, ptr - cur);
  ParseInt64(dst_sv, dst);
  cur = ptr + 1;

  ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  prop = std::string_view(cur, ptr - cur);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::string& prop) {
  const char* cur = line;
  const char* ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  std::string_view src_sv(cur, ptr - cur);
  ParseInt64(src_sv, src);
  cur = ptr + 1;

  ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  std::string_view dst_sv(cur, ptr - cur);
  ParseInt64(dst_sv, dst);
  cur = ptr + 1;

  ptr = cur;
  while (*ptr != '\0' && *ptr != '|') {
    ++ptr;
  }
  prop = std::string(cur, ptr - cur);
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value) {
  PropertyType type = value.type_;
  in_archive << type;
  switch (type) {
  case PropertyType::kEmpty:
    break;
  case PropertyType::kInt8:
    in_archive << value.value.i8;
    break;
  case PropertyType::kUInt8:
    in_archive << value.value.u8;
    break;
  case PropertyType::kInt16:
    in_archive << value.value.i16;
    break;
  case PropertyType::kUInt16:
    in_archive << value.value.u16;
    break;
  case PropertyType::kInt32:
    in_archive << value.value.i32;
    break;
  case PropertyType::kUInt32:
    in_archive << value.value.u32;
    break;
  case PropertyType::kInt64:
    in_archive << value.value.i64;
    break;
  case PropertyType::kUInt64:
    in_archive << value.value.u64;
    break;
  case PropertyType::kDate:
    in_archive << value.value.dt.milli_second;
    break;
  case PropertyType::kFloat:
    in_archive << value.value.f;
    break;
  case PropertyType::kDouble:
    in_archive << value.value.db;
    break;
  case PropertyType::kString:
    in_archive << value.value.s;
    // value.get_value<std::string>();
    break;
  case PropertyType::kStringView:
    in_archive << value.value.sw;
    break;
  case PropertyType::kList:
    in_archive << value.value.vec;
    // get_value<std::vector<Property>>();
    break;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value) {
  // PropertyType type;
  out_archive >> value.type_;
  switch (value.type_) {
  case PropertyType::kEmpty:
    value.set_value(grape::EmptyType());
    break;
  case PropertyType::kInt8:
    out_archive >> value.value.i8;
    break;
  case PropertyType::kUInt8:
    out_archive >> value.value.u8;
    break;
  case PropertyType::kInt16:
    out_archive >> value.value.i16;
    break;
  case PropertyType::kUInt16:
    out_archive >> value.value.u16;
    break;
  case PropertyType::kInt32:
    out_archive >> value.value.i32;
    break;
  case PropertyType::kUInt32:
    out_archive >> value.value.u32;
    break;
  case PropertyType::kInt64:
    out_archive >> value.value.i64;
    break;
  case PropertyType::kUInt64:
    out_archive >> value.value.u64;
    break;
  case PropertyType::kDate:
    out_archive >> value.value.dt.milli_second;
    break;
  case PropertyType::kFloat:
    out_archive >> value.value.f;
    break;
  case PropertyType::kDouble:
    out_archive >> value.value.db;
    break;
  case PropertyType::kString: {
    std::string s;
    out_archive >> s;
    value.set_value(std::move(s));
  } break;
  case PropertyType::kStringView:
    out_archive >> value.value.sw;
    break;
  case PropertyType::kList: {
    std::vector<Property> list;
    out_archive >> list;
    value.set_value(std::move(list));
  } break;
  }

  return out_archive;
}

// date format:
// YYYY-MM-DD'T'hh:mm:ss.SSSZZZZ
// 2010-04-25T05:45:11.772+0000

inline static uint32_t char_to_digit(char c) { return (c - '0'); }

inline static uint32_t str_4_to_number(const char* str) {
  return char_to_digit(str[0]) * 1000u + char_to_digit(str[1]) * 100u +
         char_to_digit(str[2]) * 10u + char_to_digit(str[3]);
}

inline static uint32_t str_3_to_number(const char* str) {
  return char_to_digit(str[0]) * 100u + char_to_digit(str[1]) * 10u +
         char_to_digit(str[2]);
}

inline static uint32_t str_2_to_number(const char* str) {
  return char_to_digit(str[0]) * 10u + char_to_digit(str[1]);
}

// Date::Date(const Date& x) : milli_second(x.milli_second) {}
Date::Date(int64_t x) : milli_second(x) {}
Date::Date(const char* str) { reset(str); }

void Date::reset(const char* str) {
  if (str[4] == '-') {
    struct tm v;
    memset(&v, 0, sizeof(v));
    v.tm_year = str_4_to_number(str) - 1900;
    v.tm_mon = str_2_to_number(&str[5]) - 1;
    v.tm_mday = str_2_to_number(&str[8]);
    if (str[10] == '|') {
      milli_second = mktime(&v);
      milli_second *= 1000l;
      milli_second += 8 * 60 * 60 * 1000l;
      return;
    }
    v.tm_hour = str_2_to_number(&str[11]);
    v.tm_min = str_2_to_number(&str[14]);
    v.tm_sec = str_2_to_number(&str[17]);

    milli_second = (mktime(&v));

    milli_second *= 1000l;
    milli_second += str_3_to_number(&str[20]);
    bool zone_flag = (str[23] == '+') ? 1u : 0u;
    uint32_t zone_hour = str_2_to_number(&str[24]);
    uint32_t zone_minute = str_2_to_number(&str[26]);
    milli_second += 8 * 60 * 60 * 1000l;
    if (zone_flag) {
      milli_second += (zone_hour * 60 * 60l + zone_minute * 60l) * 1000l;
    } else {
      milli_second -= (zone_hour * 60 * 60l + zone_minute * 60l) * 1000l;
    }
  } else {
#ifdef __APPLE__
    sscanf(str, "%lld", &milli_second);
#else
    sscanf(str, "%" SCNd64, &milli_second);
#endif
  }
}

std::string Date::to_string() const { return std::to_string(milli_second); }

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

grape::InArchive& operator<<(grape::InArchive& arc, const Date& v) {
  arc << v.milli_second;
  return arc;
}
grape::OutArchive& operator>>(grape::OutArchive& arc, Date& v) {
  arc >> v.milli_second;
  return arc;
}

}  // namespace gs
