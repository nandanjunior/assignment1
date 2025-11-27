#include "genre_analysis_logic.h"
#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <string>
#include <thread>
#include <mutex>

GenreAnalysisResult GenreAnalysisLogic::perform_genre_analysis(const std::vector<std::string>& genres) {
    auto start = std::chrono::steady_clock::now();

    std::unordered_map<std::string,int> counts;
    std::mutex mtx; // for thread-safe updates

    // Parallel map-like counting (like ThreadPoolExecutor)
    auto worker = [&](size_t start_idx, size_t end_idx){
        std::unordered_map<std::string,int> local_counts;
        for (size_t i = start_idx; i < end_idx; ++i)
            local_counts[genres[i]]++;
        // Merge local counts to global map
        std::lock_guard<std::mutex> lock(mtx);
        for (auto &p : local_counts)
            counts[p.first] += p.second;
    };

    const size_t num_threads = std::min<size_t>(4, genres.size());
    std::vector<std::thread> threads;
    size_t block_size = genres.size() / num_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start_idx = t * block_size;
        size_t end_idx = (t == num_threads-1) ? genres.size() : start_idx + block_size;
        threads.emplace_back(worker, start_idx, end_idx);
    }
    for (auto &th : threads) th.join();

    // Sort by count descending
    std::vector<std::pair<std::string,int>> sorted_counts(counts.begin(), counts.end());
    std::sort(sorted_counts.begin(), sorted_counts.end(),
              [](const auto &a, const auto &b){ return a.second > b.second; });

    std::vector<std::string> top_genres;
    for (auto &p : sorted_counts) top_genres.push_back(p.first);

    auto end = std::chrono::steady_clock::now();
    double processing_time = std::chrono::duration<double>(end - start).count();

    return {counts, top_genres, processing_time};
}
