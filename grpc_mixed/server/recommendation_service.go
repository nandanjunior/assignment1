package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "grpc_mixed/server/generated"
	backend "grpc_mixed/go_service"

	"google.golang.org/grpc"
)

// RecommendationHandler implements the gRPC server
type RecommendationHandler struct {
	pb.UnimplementedRecommendationServiceServer
}

// Recommend handles the recommendation request
func (s *RecommendationHandler) Recommend(ctx context.Context, req *pb.RecommendationRequest) (*pb.RecommendationResponse, error) {
	start := time.Now()

	// Convert gRPC user stats to backend struct
	playCounts := req.PlayCounts.PlayCounts
	var users []backend.UserStats
	for _, u := range req.UserStats.UserStats {
		users = append(users, backend.UserStats{
			UserID:    u.UserId,
			TopArtist: u.TopArtist,
		})
	}

	// Call backend recommendation logic
	result := (&backend.RecommendationService{}).Recommend(playCounts, users)

	// Set processing time
	result.ProcessingTime = time.Since(start).Seconds()

	// Prepare gRPC response
	resp := &pb.RecommendationResponse{
		ProcessingTime:  result.ProcessingTime,
		TrendingSongs:   result.TrendingSongs,
		Recommendations: map[string]*pb.RepeatedString{},
	}

	for uid, recs := range result.Recommendations {
		resp.Recommendations[uid] = &pb.RepeatedString{Values: recs}
	}

	// Save metrics to local JSON
	saveMetrics("/tmp/results_recommendation_go.json", map[string]interface{}{
		"processing_time": result.ProcessingTime,
		"num_trending":    len(result.TrendingSongs),
	})

	log.Println("Request processed in:", result.ProcessingTime)
	return resp, nil
}

// saveMetrics writes metrics to JSON
func saveMetrics(path string, data interface{}) {
	os.MkdirAll(filepath.Dir(path), 0755)
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println("Failed to marshal metrics:", err)
		return
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		log.Println("Failed to write metrics:", err)
	}
}

const PORT = ":50057"

func main() {
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Failed to bind port %s: %v", PORT, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRecommendationServiceServer(grpcServer, &RecommendationHandler{})

	log.Println("🚀 Go Recommendation Service running on", PORT)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Server error:", err)
	}
}
