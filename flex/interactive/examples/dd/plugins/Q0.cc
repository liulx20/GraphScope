#include <queue>
#include <random>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"
namespace gs {

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
class Query0 : public AppBase {
 public:
  Query0(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        workat_label_id_(graph_.schema().get_edge_label_id("WorkAt")),
        friend_label_id_(graph_.schema().get_edge_label_id("Friend")),
        chat_in_group_label_id_(
            graph_.schema().get_edge_label_id("ChatInGroup")),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        org_num_(graph.graph().vertex_num(ding_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        mt_(random_device_()) {}

  void get_friends(vid_t root, gs::ImmutableGraphView<grape::EmptyType>& oes,
                   gs::ImmutableGraphView<grape::EmptyType>& ies,
                   std::vector<vid_t>& friends) {
    const auto& oe = oes.get_edges(root);
    friends.clear();
    for (auto& e : oe) {
      friends.emplace_back(e.neighbor);
    }
    const auto& ie = ies.get_edges(root);
    for (auto& e : ie) {
      friends.emplace_back(e.neighbor);
    }
    std::sort(friends.begin(), friends.end());
  }

  bool check_same_org(const std::unordered_set<uint32_t>& st,
                      const GraphView<char_array<20>>& view, uint32_t root,
                      std::unordered_map<uint32_t, bool>& mem) {
    if (mem.count(root))
      return mem[root];

    const auto& ie = view.get_edges(root);
    mem[root] = false;
    for (auto& e : ie) {
      if (st.count(e.neighbor)) {
        mem[root] = true;
        break;
      }
    }
    return mem[root];
  }
  bool Query(Decoder& input, Encoder& output) {
    static constexpr int RETURN_LIMIT = 20;
    int64_t oid = input.get_long();
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    const auto& workat_edges = txn.GetOutgoingEdges<char_array<20>>(
        user_label_id_, root, ding_org_label_id_, workat_label_id_);
    const auto& workat_ie = txn.GetIncomingGraphView<char_array<20>>(
        ding_org_label_id_, user_label_id_, workat_label_id_);
    std::unordered_set<vid_t> orgs;
    size_t sum = 0;
    for (auto& e : workat_edges) {
      int d = workat_ie.get_edges(e.neighbor).estimated_degree();
      if (d <= 1) {
        continue;
      }
      orgs.emplace(e.get_neighbor());  // org
      sum += d;
    }
    if (sum == 0) {
      output.put_int(0);
      return true;
    }

    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;

    for (auto& e : intimacy_edges) {
      intimacy_users.emplace_back(e.get_neighbor());
    }
    std::sort(intimacy_users.begin(), intimacy_users.end());

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
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
      intimacy_users.resize(len);
    }

    auto workat_oe = txn.GetOutgoingGraphView<char_array<20>>(
        user_label_id_, ding_org_label_id_, workat_label_id_);
    std::unordered_map<uint32_t, bool> mem;
    std::vector<vid_t> intimacy_users_tmp;
    // root -> Intimacy -> Users
    for (auto& v : intimacy_users) {
      if (check_same_org(orgs, workat_oe, v, mem) && !vis_set.count(v)) {
        intimacy_users_tmp.emplace_back(v);
      }
    }
    std::vector<int64_t> return_vec;
    if (intimacy_users_tmp.size() <= RETURN_LIMIT / 5) {
      //@TODO 推荐理由
      for (auto user : intimacy_users_tmp) {
        return_vec.emplace_back(user);
        vis_set.emplace(user);
      }
    } else {
      // random pick
      std::uniform_int_distribution<int> dist(0, intimacy_users_tmp.size() - 1);
      std::unordered_set<int> st;
      for (int i = 0; i < RETURN_LIMIT / 5; ++i) {
        int ind = dist(mt_);
        while (st.count(ind)) {
          ++ind;
          ind %= intimacy_users_tmp.size();
        }
        st.emplace(ind);
        return_vec.emplace_back(intimacy_users_tmp[ind]);
        vis_set.emplace(intimacy_users_tmp[ind]);
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
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
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
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
            common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor] == 1 &&
                common_group_users.count(e.neighbor) == 0) {
              common_users_vec.emplace_back(e.neighbor);
            }
          }
        }
        for (auto& e : oe) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
            common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor] == 1 &&
                common_group_users.count(e.neighbor) == 0) {
              common_users_vec.emplace_back(e.neighbor);
            }
          }
        }
      }
    }
    int res = RETURN_LIMIT - return_vec.size();

    // less than 20 users
    if (common_users_vec.size() <= res) {
      std::shuffle(common_users_vec.begin(), common_users_vec.end(), mt_);
      for (auto id : common_users_vec) {
        return_vec.emplace_back(id);
      }

    } else {
      std::uniform_int_distribution<int> dist(0, common_users_vec.size() - 1);
      std::unordered_set<int> st;
      for (int i = 0; i < res; ++i) {
        int ind = dist(mt_);
        while (st.count(ind)) {
          ind++;
          ind %= common_users_vec.size();
        }
        st.emplace(ind);
        return_vec.emplace_back(common_users_vec[ind]);
      }
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
  label_t workat_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t study_at_label_id_;

  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;
  std::random_device random_device_;
  std::mt19937 mt_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}
