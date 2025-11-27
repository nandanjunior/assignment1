cmake_minimum_required(VERSION 3.15)
project(GenreAnalysisCppService)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use CONFIG mode to avoid multiple target definitions
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

include_directories(
    ${Protobuf_INCLUDE_DIRS}
    ../server/generated_cpp
    .
)

add_executable(genre_analysis_server
    ../server/genre_analysis_service.cpp
    genre_analysis_logic.cpp
    ../server/generated_cpp/music_service.pb.cc
    ../server/generated_cpp/music_service.grpc.pb.cc
)

target_link_libraries(genre_analysis_server
    gRPC::grpc++
    protobuf::libprotobuf
)
