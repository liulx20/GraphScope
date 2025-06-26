#include "flex/engines/graph_db/chunked_runtime/common/ops/retrieve/path_expand.h"
#include "flex/engines/graph_db/chunked_runtime/common/datachunks/path_columns.h"

namespace gs {
namespace chunked_runtime {
namespace ops {
using gs::runtime::Path;
using gs::runtime::PathImpl;
bl::result<void> PathExpand::edge_expand_v(const GraphReadInterface& graph,
                                           const DataChunk& ctx,
                                           const PathExpandParams& params,
                                           LocalEdgeExpandState& state) {
  if (params.labels.size() == 1 &&
      ctx.get_vertex_column_type(params.start_tag) ==
          VertexColumnType::kSingle) {
    return path_expand_vertex_without_predicate_impl(
        graph, ctx, params.start_tag, params.labels, params.dir,
        params.hop_lower, params.hop_upper, state);
  } else {
    if (params.dir == Direction::kOut) {
      std::unordered_set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> out_labels_map(
          graph.schema().vertex_label_num());
      for (const auto& label : params.labels) {
        labels.emplace(label.dst_label);
        out_labels_map[label.src_label].emplace_back(label);
      }

      auto builder =
          state.getEdgeCollector<MLVertexColumn, std::unordered_set<label_t>>(
              labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      ctx.foreach_vertex(params.start_tag,
                         [&](size_t index, label_t label, vid_t v) {
                           output.emplace_back(label, v, index);
                         });
      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_opt(std::get<2>(tuple), std::get<0>(tuple),
                                  std::get<1>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : out_labels_map[label]) {
            auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.dst_label, nbr, index);
              oe_iter.Next();
            }
          }
        }
        ++depth;
      }
      return bl::result<void>();
    } else if (params.dir == Direction::kIn) {
      std::unordered_set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> in_labels_map(
          graph.schema().vertex_label_num());
      for (auto& label : params.labels) {
        labels.emplace(label.src_label);
        in_labels_map[label.dst_label].emplace_back(label);
      }

      auto builder =
          state.getEdgeCollector<MLVertexColumn, std::unordered_set<label_t>>(
              labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      ctx.foreach_vertex(params.start_tag,
                         [&](size_t index, label_t label, vid_t v) {
                           output.emplace_back(label, v, index);
                         });
      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (const auto& tuple : input) {
            builder.push_back_opt(std::get<2>(tuple), std::get<0>(tuple),
                                  std::get<1>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (const auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : in_labels_map[label]) {
            auto oe_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.src_label, nbr, index);
              oe_iter.Next();
            }
          }
        }
        ++depth;
      }
      return bl::result<void>();
    } else if (params.dir == Direction::kBoth) {
      std::unordered_set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> in_labels_map(
          graph.schema().vertex_label_num()),
          out_labels_map(graph.schema().vertex_label_num());
      for (const auto& label : params.labels) {
        labels.emplace(label.dst_label);
        in_labels_map[label.dst_label].emplace_back(label);
        out_labels_map[label.src_label].emplace_back(label);
      }

      auto builder =
          state.getEdgeCollector<MLVertexColumn, std::unordered_set<label_t>>(
              labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;

      ctx.foreach_vertex(params.start_tag,
                         [&](size_t index, label_t label, vid_t v) {
                           output.emplace_back(label, v, index);
                         });

      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_opt(std::get<2>(tuple), std::get<0>(tuple),
                                  std::get<1>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : out_labels_map[label]) {
            auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.dst_label, nbr, index);
              oe_iter.Next();
            }
          }
          for (const auto& label_triplet : in_labels_map[label]) {
            auto ie_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              output.emplace_back(label_triplet.src_label, nbr, index);
              ie_iter.Next();
            }
          }
        }
        depth++;
      }

      return bl::result<void>();
    }
  }
  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

bl::result<void> PathExpand::edge_expand_p(const GraphReadInterface& graph,
                                           const DataChunk& ctx,
                                           const PathExpandParams& params,
                                           LocalEdgeExpandState& state) {
  const auto& label_sets = ctx.get_vertex_labels_set(params.start_tag);
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_num()),
      in_labels_map(graph.schema().vertex_label_num());
  for (const auto& triplet : labels) {
    out_labels_map[triplet.src_label].emplace_back(triplet);
    in_labels_map[triplet.dst_label].emplace_back(triplet);
  }
  auto dir = params.dir;
  std::vector<std::pair<std::unique_ptr<PathImpl>, size_t>> input;
  std::vector<std::pair<std::unique_ptr<PathImpl>, size_t>> output;

  auto builder = state.getEdgeCollector<GeneralPathColumn>();
  std::shared_ptr<Arena> arena = std::make_shared<Arena>();
  if (dir == Direction::kOut) {
    ctx.foreach_vertex(params.start_tag,
                       [&](size_t index, label_t label, vid_t v) {
                         auto p = PathImpl::make_path_impl(label, v);
                         input.emplace_back(std::move(p), index);
                       });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth + 1 < params.hop_upper) {
        for (auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oe_iter = graph.GetOutEdgeIterator(end.label_, end.vid_,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              std::unique_ptr<PathImpl> new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              oe_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(index, std::move(path));
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    return bl::result<void>();
  } else if (dir == Direction::kIn) {
    ctx.foreach_vertex(params.start_tag,
                       [&](size_t index, label_t label, vid_t v) {
                         auto p = PathImpl::make_path_impl(label, v);
                         input.emplace_back(std::move(p), index);
                       });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();

      if (depth + 1 < params.hop_upper) {
        for (const auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : in_labels_map[end.label_]) {
            auto ie_iter = graph.GetInEdgeIterator(end.label_, end.vid_,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              std::unique_ptr<PathImpl> new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              ie_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(index, std::move(path));
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    return bl::result<void>();

  } else if (dir == Direction::kBoth) {
    ctx.foreach_vertex(params.start_tag,
                       [&](size_t index, label_t label, vid_t v) {
                         auto p = PathImpl::make_path_impl(label, v);
                         input.emplace_back(std::move(p), index);
                       });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth + 1 < params.hop_upper) {
        for (auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oe_iter = graph.GetOutEdgeIterator(end.label_, end.vid_,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              oe_iter.Next();
            }
          }

          for (const auto& label_triplet : in_labels_map[end.label_]) {
            auto ie_iter = graph.GetInEdgeIterator(end.label_, end.vid_,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              ie_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(index, std::move(path));
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    return bl::result<void>();
  }
  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

static void dfs(const GraphReadInterface& graph, vid_t src, vid_t dst,
                const GraphReadInterface::vertex_array_t<bool>& visited,
                const GraphReadInterface::vertex_array_t<int8_t>& dist,
                const ShortestPathParams& params,
                std::vector<std::vector<vid_t>>& paths,
                std::vector<vid_t>& cur_path) {
  cur_path.push_back(src);
  if (src == dst) {
    paths.emplace_back(cur_path);
    cur_path.pop_back();
    return;
  }
  auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, src,
                                          params.labels[0].dst_label,
                                          params.labels[0].edge_label);
  while (oe_iter.IsValid()) {
    vid_t nbr = oe_iter.GetNeighbor();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      dfs(graph, nbr, dst, visited, dist, params, paths, cur_path);
    }
    oe_iter.Next();
  }
  auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, src,
                                         params.labels[0].src_label,
                                         params.labels[0].edge_label);
  while (ie_iter.IsValid()) {
    vid_t nbr = ie_iter.GetNeighbor();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      dfs(graph, nbr, dst, visited, dist, params, paths, cur_path);
    }

    ie_iter.Next();
  }
  cur_path.pop_back();
}

static void all_shortest_path_with_given_source_and_dest_impl(
    const GraphReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<std::vector<vid_t>>& paths) {
  GraphReadInterface::vertex_array_t<int8_t> dist_from_src(
      graph.GetVertexSet(params.labels[0].src_label), -1);
  GraphReadInterface::vertex_array_t<int8_t> dist_from_dst(
      graph.GetVertexSet(params.labels[0].dst_label), -1);
  dist_from_src[src] = 0;
  dist_from_dst[dst] = 0;
  std::queue<vid_t> q1, q2, tmp;
  q1.push(src);
  q2.push(dst);
  std::vector<vid_t> vec;
  int8_t src_dep = 0, dst_dep = 0;

  while (true) {
    if (src_dep >= params.hop_upper || dst_dep >= params.hop_upper ||
        !vec.empty()) {
      break;
    }
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        vid_t v = q1.front();
        q1.pop();
        auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, v,
                                                params.labels[0].dst_label,
                                                params.labels[0].edge_label);
        while (oe_iter.IsValid()) {
          vid_t nbr = oe_iter.GetNeighbor();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, v,
                                               params.labels[0].src_label,
                                               params.labels[0].edge_label);
        while (ie_iter.IsValid()) {
          vid_t nbr = ie_iter.GetNeighbor();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          ie_iter.Next();
        }
      }
      std::swap(q1, tmp);
      ++src_dep;
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        vid_t v = q2.front();
        q2.pop();
        auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].dst_label, v,
                                                params.labels[0].src_label,
                                                params.labels[0].edge_label);
        while (oe_iter.IsValid()) {
          vid_t nbr = oe_iter.GetNeighbor();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(params.labels[0].src_label, v,
                                               params.labels[0].dst_label,
                                               params.labels[0].edge_label);
        while (ie_iter.IsValid()) {
          vid_t nbr = ie_iter.GetNeighbor();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          ie_iter.Next();
        }
      }
      std::swap(q2, tmp);
      ++dst_dep;
    }
  }

  while (!q1.empty()) {
    q1.pop();
  }
  if (vec.empty()) {
    return;
  }
  if (src_dep + dst_dep >= params.hop_upper) {
    return;
  }
  GraphReadInterface::vertex_array_t<bool> visited(
      graph.GetVertexSet(params.labels[0].src_label), false);
  for (auto v : vec) {
    q1.push(v);
    visited[v] = true;
  }
  while (!q1.empty()) {
    auto v = q1.front();
    q1.pop();
    auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, v,
                                            params.labels[0].dst_label,
                                            params.labels[0].edge_label);
    while (oe_iter.IsValid()) {
      vid_t nbr = oe_iter.GetNeighbor();
      if (visited[nbr]) {
        oe_iter.Next();
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
      oe_iter.Next();
    }

    auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, v,
                                           params.labels[0].src_label,
                                           params.labels[0].edge_label);
    while (ie_iter.IsValid()) {
      vid_t nbr = ie_iter.GetNeighbor();
      if (visited[nbr]) {
        ie_iter.Next();
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
      ie_iter.Next();
    }
  }
  std::vector<vid_t> cur_path;
  dfs(graph, src, dst, visited, dist_from_src, params, paths, cur_path);
}

bl::result<void> PathExpand::all_shortest_paths_with_given_source_and_dest(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const ShortestPathParams& params, const std::pair<label_t, vid_t>& dest,
    LocalPathState& state) {
  auto label_sets = ctx.get_vertex_labels_set(params.start_tag);
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }
  auto dir = params.dir;
  if (dir != Direction::kBoth) {
    LOG(ERROR) << "only support both direction";
    RETURN_UNSUPPORTED_ERROR("only support both direction");
  }

  if (dest.first != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }

  ctx.foreach_vertex(
      params.start_tag, [&](size_t index, label_t label, vid_t v) {
        std::vector<std::vector<vid_t>> paths;
        all_shortest_path_with_given_source_and_dest_impl(graph, params, v,
                                                          dest.second, paths);
        for (auto& path : paths) {
          auto ptr = PathImpl::make_path_impl(label_triplet.src_label,
                                              label_triplet.edge_label, path);
          state.push_back(index, dest.second, std::move(ptr));
        }
      });
  return bl::result<void>();
}

static bool single_source_single_dest_shortest_path_impl(
    const GraphReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<vid_t>& path) {
  std::queue<vid_t> q1;
  std::queue<vid_t> q2;
  std::queue<vid_t> tmp;

  label_t v_label = params.labels[0].src_label;
  label_t e_label = params.labels[0].edge_label;
  auto vertices = graph.GetVertexSet(v_label);
  GraphReadInterface::vertex_array_t<int> pre(vertices, -1);
  GraphReadInterface::vertex_array_t<int> dis(vertices, 0);
  q1.push(src);
  dis[src] = 1;
  q2.push(dst);
  dis[dst] = -1;

  while (true) {
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        int x = q1.front();
        if (dis[x] >= params.hop_upper + 1) {
          return false;
        }
        q1.pop();
        auto oe_iter = graph.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q1, tmp);
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        int x = q2.front();
        if (dis[x] <= -params.hop_upper - 1) {
          return false;
        }
        q2.pop();
        auto oe_iter = graph.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q2, tmp);
    }
  }
  return false;
}

bl::result<void> PathExpand::single_source_single_dest_shortest_path(
    const GraphReadInterface& graph, const DataChunk& ctx,
    const ShortestPathParams& params, std::pair<label_t, vid_t>& dest,
    LocalPathState& state) {
  auto label_sets = ctx.get_vertex_labels_set(params.start_tag);
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label ||
      params.dir != Direction::kBoth) {
    LOG(ERROR) << "only support same src and dst label and both direction";
    RETURN_UNSUPPORTED_ERROR(
        "only support same src and dst label and both "
        "direction");
  }

  ctx.foreach_vertex(
      params.start_tag, [&](size_t index, label_t label, vid_t v) {
        std::vector<vid_t> path;
        if (single_source_single_dest_shortest_path_impl(graph, params, v,
                                                         dest.second, path)) {
          auto impl = PathImpl::make_path_impl(label_triplet.src_label,
                                               label_triplet.edge_label, path);
          state.push_back(index, dest.second, std::move(impl));
        }
      });
  return bl::result<void>();
}

}  // namespace ops
}  // namespace chunked_runtime
}  // namespace gs