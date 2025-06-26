file(GLOB_RECURSE COMMON_SOURCES "common/*.cc")
add_library(chunked_runtime_common SHARED ${COMMON_SOURCES})
target_link_libraries(chunked_runtime_common flex_rt_mutable_graph flex_plan_proto)
install_flex_target(chunked_runtime_common)

file(GLOB_RECURSE EXECUTE_SOURCES "execute/*.cc" "utils/*.cc")
add_library(chunked_runtime_execute SHARED ${EXECUTE_SOURCES})
target_link_libraries(chunked_runtime_execute chunked_runtime_common)
install_flex_target(chunked_runtime_execute)