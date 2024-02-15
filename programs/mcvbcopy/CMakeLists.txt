add_executable(mcvbcopy
        mcvbcopy.cc
        $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mcvbcopy)
target_link_libraries(mcvbcopy PRIVATE snapshot_download mc_client_connection platform spdlog::spdlog)
add_sanitizers(mcvbcopy)
install(TARGETS mcvbcopy RUNTIME DESTINATION bin)
