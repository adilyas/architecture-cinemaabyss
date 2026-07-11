package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"

	consumerGroup = "events-service"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

var brokers []string

func main() {
	brokers = strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")

	for _, topic := range []string{topicMovie, topicUser, topicPayment} {
		startConsumer(topic)
	}

	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleEvent(topicMovie, "movie"))
	http.HandleFunc("/api/events/user", handleEvent(topicUser, "user"))
	http.HandleFunc("/api/events/payment", handleEvent(topicPayment, "payment"))

	port := env("PORT", "8082")
	log.Printf("Starting events service on port %s (brokers=%s)", port, strings.Join(brokers, ","))
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleEvent(topic, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		event := Event{
			ID:        fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano()),
			Type:      eventType,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		}

		value, err := json.Marshal(event)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		partition, offset, err := produce(topic, []byte(event.ID), value)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		log.Printf("produced %s event to %s partition %d offset %d", eventType, topic, partition, offset)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(EventResponse{
			Status:    "success",
			Partition: partition,
			Offset:    offset,
			Event:     event,
		})
	}
}

func produce(topic string, key, value []byte) (int, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.WriteMessages(kafka.Message{Key: key, Value: value}); err != nil {
		return 0, 0, err
	}
	return 0, offset, nil
}

func startConsumer(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: consumerGroup,
	})
	go func() {
		for {
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("consumer error on %s: %v", topic, err)
				time.Sleep(time.Second)
				continue
			}
			log.Printf("consumed from %s partition %d offset %d: %s", m.Topic, m.Partition, m.Offset, string(m.Value))
		}
	}()
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
