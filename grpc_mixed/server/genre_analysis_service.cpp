#include <grpcpp/grpcpp.h>
#include "generated_cpp/music_service.grpc.pb.h"
#include "../cpp_service/genre_analysis_logic.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using music::StreamList;
using music::GenreAnalysisResponse;
using music::GenreAnalysisService;

class GenreAnalysisServiceImpl final : public GenreAnalysisService::Service {
public:
    Status AnalyzeGenres(ServerContext* context, const StreamList* request,
                         GenreAnalysisResponse* response) override {
        std::vector<std::string> genres;
        for (const auto& rec : request->records()) genres.push_back(rec.genre());

        auto result = GenreAnalysisLogic::perform_genre_analysis(genres);

        for (auto& [genre,count] : result.genre_counts)
            (*response->mutable_genre_counts())[genre] = count;

        for (auto& genre : result.top_genres) response->add_top_genres(genre);
        response->set_processing_time(result.processing_time);

        return Status::OK;
    }
};

int main() {
    std::string address("0.0.0.0:50055");
    GenreAnalysisServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[GenreAnalysis] C++ server listening on " << address << std::endl;

    server->Wait();
    return 0;
}
