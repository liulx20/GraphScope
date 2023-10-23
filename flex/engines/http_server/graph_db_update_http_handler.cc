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
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/graph_db_update_service.h"
#include "flex/engines/http_server/options.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>
#include "flex/engines/http_server/generated/executor_ref.act.autogen.h"
#include "flex/engines/http_server/graph_db_update_server.h"
#include "flex/engines/http_server/types.h"

namespace server {

class update_query_handler : public seastar::httpd::handler_base {
 public:
  update_query_handler(uint32_t group_id, uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    executor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
    }
  }
  ~update_query_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    if (GraphDBUpdateService::get().forward()) {
      req->content[req->content.size() - 1] += (1 << 7);
    }
    return executor_refs_[dst_executor]
        .run_graph_db_update_query(query_param{std::move(req->content)})
        .then_wrapped([rep = std::move(rep)](
                          seastar::future<query_result>&& fut) mutable {
          if (__builtin_expect(fut.failed(), false)) {
            rep->set_status(
                seastar::httpd::reply::status_type::internal_server_error);
            try {
              std::rethrow_exception(fut.get_exception());
            } catch (std::exception& e) {
              rep->write_body("bin", seastar::sstring(e.what()));
            }
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          }
          auto result = fut.get0();
          rep->write_body("bin", std::move(result.content));
          rep->done();
          return seastar::make_ready_future<
              std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
};

class update_exit_handler : public seastar::httpd::handler_base {
 public:
  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    GraphDBUpdateServer::get().Forward();
    GraphDBUpdateService::get().set_exit_state();
    rep->write_body(
        "bin",
        seastar::sstring{"The ldbc snb interactive service is exiting ..."});
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
};

graph_db_update_http_handler::graph_db_update_http_handler(uint16_t http_port)
    : http_port_(http_port) {}

void graph_db_update_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print(
                  "Ldbc snb interactive http handler is listening on port {} "
                  "...\n",
                  http_port_);
            });
      });
  fut.wait();
}

void graph_db_update_http_handler::stop() {
  auto fut =
      seastar::alien::submit_to(*seastar::alien::internal::default_instance, 0,
                                [this] { return server_.stop(); });
  fut.wait();
}

seastar::future<> graph_db_update_http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    r.add(
        seastar::httpd::operation_type::POST,
        seastar::httpd::url("/interactive/update"),
        new update_query_handler(ic_update_group_id, shard_update_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/exit"), new update_exit_handler());
    return seastar::make_ready_future<>();
  });
}

}  // namespace server
