package services

import (
	"sort"
	"time"
)

type UserStats struct {
	UserID    string
	TopArtist string
}

type RecommendationResult struct {
	TrendingSongs   []string
	Recommendations map[string][]string
	ProcessingTime  float64
}

type RecommendationService struct{}

func (rs *RecommendationService) Recommend(playCounts map[string]int32, userStats []UserStats) RecommendationResult {
	start := time.Now()

	type kv struct {
		Key   string
		Value int32
	}
	var countSlice []kv
	for k, v := range playCounts {
		countSlice = append(countSlice, kv{k, v})
	}

	sort.Slice(countSlice, func(i, j int) bool {
		return countSlice[i].Value > countSlice[j].Value
	})

	trending := []string{}
	for i := 0; i < len(countSlice) && i < 5; i++ {
		trending = append(trending, countSlice[i].Key)
	}

	recommendations := make(map[string][]string)
	for _, u := range userStats {
		list := []string{}
		for _, song := range trending {
			if u.TopArtist != "" && !containsSubstring(song, u.TopArtist) {
				list = append(list, song)
			}
		}
		recommendations[u.UserID] = list
	}

	return RecommendationResult{
		TrendingSongs:   trending,
		Recommendations: recommendations,
		ProcessingTime:  time.Since(start).Seconds(),
	}
}

func containsSubstring(s, sub string) bool {
	return len(sub) > 0 && len(s) > 0 && Index(s, sub) != -1
}

func Index(str, substr string) int {
outer:
	for i := 0; i+len(substr) <= len(str); i++ {
		for j := range substr {
			if str[i+j] != substr[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
