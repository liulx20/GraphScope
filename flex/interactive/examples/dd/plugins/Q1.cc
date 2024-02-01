#include <queue>
#include <random>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"
namespace gs {

static constexpr int32_t EMPLOYEE_CNT = 100000;

enum RecomReasonType {
  kClassMateAndColleague = 0,  // 同学兼同事
  kExColleague = 1,            // 前同事
  kCommonFriend = 2,           // 共同好友
  kCommonGroup = 3,            // 共同群
  kCommunication = 4,          // 最近沟通
  kActiveUser = 5,             // 活跃用户
  kDefault = 6                 // 默认
};

struct RecomReason {
  RecomReason() : type(kDefault) {}
  RecomReasonType type;
  int32_t num_common_group_or_friend;
  std::vector<vid_t> common_group_or_friend;
  int32_t org_id;  // 同事或者同学的组织id
};

// Recommend alumni for the input user.
class Query2 : public AppBase {
 public:
  Query2(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        subIndustry_label_id_(
            graph_.schema().get_vertex_label_id("SubIndustry")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        study_at_label_id_(graph_.schema().get_edge_label_id("StudyAt")),
        work_at_label_id_(graph_.schema().get_edge_label_id("WorkAt")),
        friend_label_id_(graph_.schema().get_edge_label_id("Friend")),
        involved_label_id_(graph_.schema().get_edge_label_id("Involved")),

        chat_in_group_label_id_(
            graph_.schema().get_edge_label_id("ChatInGroup")),
        user_subIndustry_col_(
            *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(
                graph.get_vertex_property_column(user_label_id_,
                                                 "subIndustry")))),
        user_city_col_(
            *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(
                graph.get_vertex_property_column(user_label_id_, "city")))),
        user_roleName_col_(*(std::dynamic_pointer_cast<TypedColumn<uint8_t>>(
            graph.get_vertex_property_column(user_label_id_, "roleName")))),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        org_num_(graph.graph().vertex_num(ding_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        mt_(random_device_()) {}

  bool check_same_org(const GraphView<char_array<16>>& study_at,
                      std::unordered_map<vid_t, int64_t>& root_study_at,
                      vid_t v, std::unordered_map<vid_t, bool>& mem) {
    if (mem.count(v))
      return mem[v];
    if (root_study_at.size()) {
      const auto& oe = study_at.get_edges(v);
      for (auto& e : oe) {
        if (root_study_at.count(e.neighbor)) {
          return mem[v] = true;
        }
      }
    }
    return mem[v] = false;
  }

  int64_t get_year_diff(int64_t a, int64_t b) {
    // a and b are seconds
    int64_t sec_per_year = 365;
    sec_per_year = sec_per_year * 24 * 3600;
    return std::abs(a - b) / sec_per_year;
  }

  bool Query(Decoder& input, Encoder& output) {
    static constexpr int RETURN_LIMIT = 20;
    int64_t oid = input.get_long();
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    auto subIndustry = user_subIndustry_col_.get_view(root);
    auto subIndustryIdx = user_subIndustry_col_.get_idx(root);
    if (subIndustry == "") {
      output.put_int(0);
      return true;
    }
    const auto& study_at = txn.GetOutgoingGraphView<char_array<16>>(
        user_label_id_, ding_edu_org_label_id_, study_at_label_id_);

    std::unordered_map<vid_t, int64_t> root_study_at;
    {
      const auto& study_oe = study_at.get_edges(root);
      for (auto& e : study_oe) {
        root_study_at[e.neighbor] =
            *reinterpret_cast<const int64_t*>(e.data.data);
      }
    }
    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;
    std::unordered_map<vid_t, bool> mem;
    for (auto& e : intimacy_edges) {
      if (check_same_org(study_at, root_study_at, e.neighbor, mem))
        intimacy_users.emplace_back(e.get_neighbor());
    }

    const auto& friend_edges_oe =
        txn.GetOutgoingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    const auto& friend_edges_ie =
        txn.GetIncomingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    std::vector<vid_t> friends;
    std::unordered_set<vid_t> vis_set;
    vis_set.emplace(root);
    // 1-hop friends
    for (auto& e : friend_edges_oe) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
    }
    for (auto& e : friend_edges_ie) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
    }
    std::sort(friends.begin(), friends.end());
    size_t friend_num =
        std::unique(friends.begin(), friends.end()) - friends.begin();
    friends.resize(friend_num);
    {
      std::sort(intimacy_users.begin(), intimacy_users.end());
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
      intimacy_users.resize(len);
      int j = 0;
      int k = 0;
      for (auto i = 0; i < intimacy_users.size(); ++i) {
        if (intimacy_users[i] == root)
          continue;
        while (j < friends.size() && friends[j] < intimacy_users[i])
          ++j;
        if (j < friends.size() && friends[j] == intimacy_users[i]) {
        } else {
          intimacy_users[k++] = intimacy_users[i];
        }
      }
      intimacy_users.resize(k);
    }

    std::vector<int64_t> return_vec;
    if (intimacy_users.size() <= RETURN_LIMIT / 5) {
      //@TODO 推荐理由
      for (auto user : intimacy_users) {
        return_vec.emplace_back(user);
        vis_set.emplace(user);
      }
    } else {
      // random pick
      std::uniform_int_distribution<int> dist(0, intimacy_users.size() - 1);
      std::unordered_set<int> st;
      for (int i = 0; i < RETURN_LIMIT / 5; ++i) {
        int ind = dist(mt_);
        while (st.count(ind)) {
          ++ind;
          ind %= intimacy_users.size();
        }
        st.emplace(ind);
        return_vec.emplace_back(intimacy_users[ind]);
        vis_set.emplace(intimacy_users[ind]);
      }
    }

    auto friends_ie = txn.GetIncomingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto friends_oe = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);

    std::unordered_set<vid_t> groups;
    // root -> oe chatInGroup -> groups
    auto group_oes = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, ding_group_label_id_, chat_in_group_label_id_);
    {
      const auto& oe = group_oes.get_edges(root);
      for (auto& e : oe) {
        groups.emplace(e.get_neighbor());
      }
    }
    // friends num + groups num,vid_t
    std::unordered_map<vid_t, int> common_group_users;
    std::vector<vid_t> common_users_vec;
    {
      // groups -> ie chatInGroup -> users
      auto group_ies = txn.GetIncomingImmutableGraphView<grape::EmptyType>(
          ding_group_label_id_, user_label_id_, chat_in_group_label_id_);
      for (auto g : groups) {
        auto d = group_ies.get_edges(g).estimated_degree();
        if (d <= 1)
          continue;
        auto ie = group_ies.get_edges(g);
        for (auto e : ie) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(study_at, root_study_at, e.neighbor, mem)) {
            common_group_users[e.neighbor] += 1;
            if (common_group_users[e.neighbor] == 1) {
              common_users_vec.emplace_back(e.neighbor);
            }
          }
        }
      }
    }
    std::unordered_map<vid_t, int> common_friend_users;
    // 2-hop friends
    if (friends.size()) {
      for (size_t i = 0; i < friends.size(); ++i) {
        auto cur = friends[i];
        const auto& ie = friends_ie.get_edges(cur);
        const auto& oe = friends_oe.get_edges(cur);
        for (auto& e : ie) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(study_at, root_study_at, e.neighbor, mem)) {
            common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor] == 1 &&
                common_group_users.count(e.neighbor) == 0) {
              common_users_vec.emplace_back(e.neighbor);
            }
          }
        }
        for (auto& e : oe) {
          if (e.neighbor != root &&
              check_same_org(study_at, root_study_at, e.neighbor, mem)) {
            common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor] == 1 &&
                common_group_users.count(e.neighbor) == 0) {
              common_users_vec.emplace_back(e.neighbor);
            }
          }
        }
      }
    }

    int res = RETURN_LIMIT * 3 / 5;
    std::shuffle(common_users_vec.begin(), common_users_vec.end(), mt_);
    int idx = common_users_vec.size();
    while (return_vec.size() < res && idx > 0) {
      auto vid = common_users_vec[idx - 1];

      if (!vis_set.count(vid)) {
        vis_set.emplace(vid);

        return_vec.emplace_back(vid);
      }
      --idx;
    }

    res = RETURN_LIMIT - return_vec.size();
    const auto& city_col =
        *std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(
            graph_.get_vertex_property_column(user_label_id_, "city"));
    auto root_city = city_col.get_view(root);
    bool exist_city = true;
    uint16_t root_city_id;
    if (root_city == " ") {
      exist_city = false;
    } else {
      root_city_id = city_col.get_idx(root);
    }
    const auto& roleName_col = *std::dynamic_pointer_cast<TypedColumn<uint8_t>>(
        graph_.get_vertex_property_column(user_label_id_, "roleName"));
    auto root_roleName = roleName_col.get_view(root);
    const auto& studyat_ie = txn.GetIncomingGraphView<char_array<16>>(
        ding_edu_org_label_id_, user_label_id_, study_at_label_id_);
    std::vector<uint32_t> tmp, cand;
    int idx1 = 0, idx2 = 0;

    // Define the range of numbers you want, here it's 0 through 99
    //     std::uniform_int_distribution<int> dist(0, 99);
    for (auto& [a, b] : root_study_at) {
      const auto& ie = studyat_ie.get_edges(a);
      for (auto& e : ie) {
        auto v = e.neighbor;
        if (vis_set.count(v)) {
          continue;
        }
        vis_set.emplace(v);
        if ((exist_city && city_col.get_idx(v) == root_city_id) ||
            (root_roleName == roleName_col.get_view(v))) {
          if (cand.size() < res) {
            cand.emplace_back(v);
          } else {
            std::uniform_int_distribution<int> dist(0, idx1);
            auto idx = dist(mt_);
            if (idx < cand.size()) {
              cand[idx] = v;
            }
          }
          idx1++;
        } else {
          int64_t joinDate = *(reinterpret_cast<const int64_t*>(e.data.data));
          if (get_year_diff(b, joinDate) <= 3) {
            if (cand.size() < res) {
              cand.emplace_back(v);
            } else {
              std::uniform_int_distribution<int> dist(0, idx1);
              auto idx = dist(mt_);
              if (idx < cand.size()) {
                cand[idx] = v;
              }
            }
            idx1++;
          } else {
            if (tmp.size() < res) {
              tmp.emplace_back(v);
            } else {
              std::uniform_int_distribution<int> dist(0, idx2);
              auto idx = dist(mt_);
              if (idx < tmp.size()) {
                tmp[idx] = v;
              }
            }
            idx2++;
          }
        }
      }
    }
    for (auto v : cand) {
      return_vec.emplace_back(v);
    }
    for (size_t i = 0; i < tmp.size() && return_vec.size() < RETURN_LIMIT;
         ++i) {
      return_vec.emplace_back(tmp[i]);
    }
    output.put_int(return_vec.size());
    for (auto vid : return_vec) {
      output.put_long(graph_.graph().get_oid(user_label_id_, vid).AsInt64());
    }

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t user_label_id_;
  label_t ding_org_label_id_;
  label_t ding_edu_org_label_id_;
  label_t ding_group_label_id_;
  label_t subIndustry_label_id_;
  label_t workat_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t work_at_label_id_;
  label_t study_at_label_id_;
  label_t involved_label_id_;

  gs::StringMapColumn<uint16_t>& user_subIndustry_col_;
  gs::StringMapColumn<uint16_t>& user_city_col_;
  gs::TypedColumn<uint8_t>& user_roleName_col_;
  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;
  std::random_device random_device_;
  std::mt19937 mt_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query2* app = new gs::Query2(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query2* casted = static_cast<gs::Query2*>(app);
  delete casted;
}
}
