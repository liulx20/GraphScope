#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_FACTORY_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_FACTORY_H_

#include "flex/storages/rt_mutable_graph/loader/csv_reader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"

namespace gs {

static void preprocess_line(char* line) {
  size_t len = strlen(line);
  while (len >= 0) {
    if (line[len] != '\0' && line[len] != '\n' && line[len] != '\r' &&
        line[len] != ' ' && line[len] != '\t') {
      break;
    } else {
      --len;
    }
  }
  line[len + 1] = '\0';
}

static std::vector<std::string> read_header(const std::string& file_name,
                                            char delimiter) {
  char line_buf[4096];
  FILE* fin = fopen(file_name.c_str(), "r");
  if (fgets(line_buf, 4096, fin) == NULL) {
    LOG(FATAL) << "Failed to read header from file: " << file_name;
  }
  preprocess_line(line_buf);
  const char* cur = line_buf;
  std::vector<std::string> res_vec;
  while (*cur != '\0') {
    const char* tmp = cur;
    while (*tmp != '\0' && *tmp != delimiter) {
      ++tmp;
    }

    std::string_view sv(cur, tmp - cur);
    res_vec.emplace_back(sv);
    cur = tmp + 1;
  }
  return res_vec;
}

inline void set_delimiter(const LoadingConfig& loading_config,
                          arrow::csv::ParseOptions& parse_options) {
  auto delimiter_str = loading_config.GetDelimiter();
  if (delimiter_str.size() != 1) {
    LOG(FATAL) << "Delimiter should be a single character";
  }
  parse_options.delimiter = delimiter_str[0];
}

inline bool set_skip_rows(const LoadingConfig& loading_config,
                          arrow::csv::ReadOptions& read_options) {
  bool header_row = loading_config.GetHasHeaderRow();
  if (header_row) {
    read_options.skip_rows = 1;
  } else {
    read_options.skip_rows = 0;
  }
  return header_row;
}

inline void set_column_names(const LoadingConfig& loading_config,
                             bool header_row, const std::string& file_path,
                             char delimiter,
                             arrow::csv::ReadOptions& read_options) {
  std::vector<std::string> all_column_names;
  if (header_row) {
    all_column_names = read_header(file_path, delimiter);
    // It is possible that there exists duplicate column names in the header,
    // transform them to unique names
    std::unordered_map<std::string, int> name_count;
    for (auto& name : all_column_names) {
      if (name_count.find(name) == name_count.end()) {
        name_count[name] = 1;
      } else {
        name_count[name]++;
      }
    }
    VLOG(10) << "before Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
    for (auto i = 0; i < all_column_names.size(); ++i) {
      auto& name = all_column_names[i];
      if (name_count[name] > 1) {
        auto cur_cnt = name_count[name];
        name_count[name] -= 1;
        all_column_names[i] = name + "_" + std::to_string(cur_cnt);
      }
    }
    VLOG(10) << "Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
  } else {
    // just get the number of columns.
    size_t num_cols = 0;
    {
      auto tmp = read_header(file_path, delimiter);
      num_cols = tmp.size();
    }
    all_column_names.resize(num_cols);
    for (auto i = 0; i < all_column_names.size(); ++i) {
      all_column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  read_options.column_names = all_column_names;
  VLOG(10) << "Got all column names: " << all_column_names.size()
           << gs::to_string(all_column_names);
}

inline void set_quote_char(const LoadingConfig& loading_config,
                           arrow::csv::ParseOptions& parse_options) {
  auto quoting_str = loading_config.GetQuotingChar();
  if (quoting_str.size() != 1) {
    LOG(FATAL) << "Quote char should be a single character";
  }
  parse_options.quote_char = quoting_str[0];
  parse_options.quoting = loading_config.GetIsQuoting();
  parse_options.double_quote = loading_config.GetIsDoubleQuoting();
}

inline void set_block_size(const LoadingConfig& loading_config,
                           arrow::csv::ReadOptions& read_options) {
  auto batch_size = loading_config.GetBatchSize();
  if (batch_size <= 0) {
    LOG(FATAL) << "Block size should be positive";
  }
  read_options.block_size = batch_size;
}

inline void fill_vertex_reader_meta(
    const Schema& schema, const LoadingConfig& loading_config, label_t v_label,
    const std::string& v_file, arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options,
    arrow::csv::ConvertOptions& convert_options) {
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());  // 2011-08-17T14:26:59.961+0000
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());

  set_delimiter(loading_config, parse_options);
  bool skip_header = set_skip_rows(loading_config, read_options);
  set_column_names(loading_config, skip_header, v_file, parse_options.delimiter,
                   read_options);
  set_quote_char(loading_config, parse_options);
  set_block_size(loading_config, read_options);

  // parse all column_names

  std::vector<std::string> included_col_names;
  std::vector<size_t> included_col_indices;
  std::vector<std::string> mapped_property_names;

  auto cur_label_col_mapping = loading_config.GetVertexColumnMappings(v_label);
  auto primary_keys = schema.get_vertex_primary_key(v_label);
  CHECK(primary_keys.size() == 1);
  auto primary_key = primary_keys[0];

  if (cur_label_col_mapping.size() == 0) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema, except for
    // primary key.
    auto primary_key_name = std::get<1>(primary_key);
    auto primary_key_ind = std::get<2>(primary_key);
    auto property_names = schema.get_vertex_property_names(v_label);
    // for example, schema is : (name,age)
    // file header is (id,name,age), the primary key is id.
    // so, the mapped_property_names are: (id,name,age)
    CHECK(property_names.size() + 1 == read_options.column_names.size());
    // insert primary_key to property_names
    property_names.insert(property_names.begin() + primary_key_ind,
                          primary_key_name);

    for (auto i = 0; i < read_options.column_names.size(); ++i) {
      included_col_names.emplace_back(read_options.column_names[i]);
      included_col_indices.emplace_back(i);
      // We assume the order of the columns in the file is the same as the
      // order of the properties in the schema, except for primary key.
      mapped_property_names.emplace_back(property_names[i]);
    }
  } else {
    for (auto i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        // use default mapping
        col_name = read_options.column_names[col_id];
      }
      included_col_names.emplace_back(col_name);
      included_col_indices.emplace_back(col_id);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include columns: " << included_col_names.size()
           << gs::to_string(included_col_names);
  // if empty, then means need all columns
  convert_options.include_columns = included_col_names;

  // put column_types, col_name : col_type
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    auto property_types = schema.get_vertex_properties(v_label);
    auto property_names = schema.get_vertex_property_names(v_label);
    CHECK(property_types.size() == property_names.size());

    for (auto i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping for "
                      "vertex label: "
                   << schema.get_vertex_label_name(v_label)
                   << " please "
                      "check your configuration";
      }
      VLOG(10) << "vertex_label: " << schema.get_vertex_label_name(v_label)
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(property_type)});
    }
    {
      // add primary key types;
      auto primary_key_name = std::get<1>(primary_key);
      auto primary_key_type = std::get<0>(primary_key);
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == primary_key_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << primary_key_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(primary_key_type)});
    }

    convert_options.column_types = arrow_types;
  }
}

inline void fill_edge_reader_meta(const Schema& schema,
                                  const LoadingConfig& loading_config,
                                  label_t src_label_id, label_t dst_label_id,
                                  label_t label_id, const std::string& e_file,
                                  arrow::csv::ReadOptions& read_options,
                                  arrow::csv::ParseOptions& parse_options,
                                  arrow::csv::ConvertOptions& convert_options) {
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());  // 2011-08-17T14:26:59.961+0000
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());

  set_delimiter(loading_config, parse_options);
  bool skip_header = set_skip_rows(loading_config, read_options);
  set_column_names(loading_config, skip_header, e_file, parse_options.delimiter,
                   read_options);
  set_quote_char(loading_config, parse_options);
  set_block_size(loading_config, read_options);

  auto src_dst_cols =
      loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);

  // parse all column_names
  // Get all column names(header, and always skip the first row)
  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  {
    // add src and dst primary col, to included_columns, put src_col and
    // dst_col at the first of included_columns.
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0];
    auto dst_col_ind = src_dst_cols.second[0];
    CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size());
    CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size());

    included_col_names.emplace_back(read_options.column_names[src_col_ind]);
    included_col_names.emplace_back(read_options.column_names[dst_col_ind]);
  }

  auto cur_label_col_mapping = loading_config.GetEdgeColumnMappings(
      src_label_id, dst_label_id, label_id);
  if (cur_label_col_mapping.empty()) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema,
    auto edge_prop_names =
        schema.get_edge_property_names(src_label_id, dst_label_id, label_id);
    for (auto i = 0; i < edge_prop_names.size(); ++i) {
      auto property_name = edge_prop_names[i];
      included_col_names.emplace_back(property_name);
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    // add the property columns into the included columns
    for (auto i = 0; i < cur_label_col_mapping.size(); ++i) {
      // TODO: make the property column's names are in same order with schema.
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        // use default mapping
        col_name = read_options.column_names[col_id];
      }
      included_col_names.emplace_back(col_name);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include Edge columns: " << gs::to_string(included_col_names);
  // if empty, then means need all columns
  convert_options.include_columns = included_col_names;

  // put column_types, col_name : col_type
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    auto property_types =
        schema.get_edge_properties(src_label_id, dst_label_id, label_id);
    auto property_names =
        schema.get_edge_property_names(src_label_id, dst_label_id, label_id);
    CHECK(property_types.size() == property_names.size());
    for (auto i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      VLOG(10) << "vertex_label: " << schema.get_edge_label_name(label_id)
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert({included_col_names[ind + 2],
                          PropertyTypeToArrowType(property_type)});
    }
    {
      // add src and dst primary col, to included_columns and column types.
      auto src_dst_cols =
          loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);
      CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
      auto src_col_ind = src_dst_cols.first[0];
      auto dst_col_ind = src_dst_cols.second[0];
      CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size());
      CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size());
      PropertyType src_col_type, dst_col_type;
      {
        auto src_primary_keys = schema.get_vertex_primary_key(src_label_id);
        CHECK(src_primary_keys.size() == 1);
        src_col_type = std::get<0>(src_primary_keys[0]);
        arrow_types.insert({read_options.column_names[src_col_ind],
                            PropertyTypeToArrowType(src_col_type)});
      }
      {
        auto dst_primary_keys = schema.get_vertex_primary_key(dst_label_id);
        CHECK(dst_primary_keys.size() == 1);
        dst_col_type = std::get<0>(dst_primary_keys[0]);
        arrow_types.insert({read_options.column_names[dst_col_ind],
                            PropertyTypeToArrowType(dst_col_type)});
      }
    }

    convert_options.column_types = arrow_types;

    VLOG(10) << "Column types: ";
    for (auto iter : arrow_types) {
      VLOG(10) << iter.first << " : " << iter.second->ToString();
    }
  }
}

inline std::shared_ptr<CSVReaderBase> create_vertex_reader(
    const Schema& schema, const LoadingConfig& loading_config, label_t v_label,
    const std::string& v_file, bool is_streaming = false) {
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fill_vertex_reader_meta(schema, loading_config, v_label, v_file, read_options,
                          parse_options, convert_options);
  if (is_streaming) {
    return std::make_shared<CSVStreamReader>(v_file, convert_options,
                                             read_options, parse_options);
  } else {
    return std::make_shared<CSVTableReader>(v_file, convert_options,
                                            read_options, parse_options);
  }
}

inline std::shared_ptr<CSVReaderBase> create_edge_reader(
    const Schema& schema, const LoadingConfig& loading_config,
    label_t src_label, label_t dst_label, label_t label,
    const std::string& e_file, bool is_streaming = false) {
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fill_edge_reader_meta(schema, loading_config, src_label, dst_label, label,
                        e_file, read_options, parse_options, convert_options);
  if (is_streaming) {
    return std::make_shared<CSVStreamReader>(e_file, convert_options,
                                             read_options, parse_options);
  } else {
    return std::make_shared<CSVTableReader>(e_file, convert_options,
                                            read_options, parse_options);
  }
}

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_READER_FACTORY_H_