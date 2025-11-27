#pragma once
#include <string>
#include <vector>
#include <unordered_map>

struct GenreAnalysisResult {
    std::unordered_map<std::string, int> genre_counts;
    std::vector<std::string> top_genres;
    double processing_time;
};

class GenreAnalysisLogic {
public:
    static GenreAnalysisResult perform_genre_analysis(const std::vector<std::string>& genres);
};
