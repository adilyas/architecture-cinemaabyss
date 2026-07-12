package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Proxy struct {
	monolith *httputil.ReverseProxy
	movies   *httputil.ReverseProxy
	events   *httputil.ReverseProxy

	gradualMigration bool
	moviesPercent    int
}

func main() {
	p := &Proxy{
		monolith:         newReverseProxy(env("MONOLITH_URL", "http://localhost:8080")),
		movies:           newReverseProxy(env("MOVIES_SERVICE_URL", "http://localhost:8081")),
		events:           newReverseProxy(env("EVENTS_SERVICE_URL", "http://localhost:8082")),
		gradualMigration: strings.EqualFold(os.Getenv("GRADUAL_MIGRATION"), "true"),
		moviesPercent:    migrationPercent(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/", p.route)

	port := env("PORT", "8000")
	log.Printf("Starting Strangler Fig proxy on port %s (gradualMigration=%t, moviesPercent=%d)",
		port, p.gradualMigration, p.moviesPercent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func (p *Proxy) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if p.routeToMovies() {
			log.Printf("movies -> movies-service: %s %s", r.Method, r.URL.Path)
			p.movies.ServeHTTP(w, r)
		} else {
			log.Printf("movies -> monolith: %s %s", r.Method, r.URL.Path)
			p.monolith.ServeHTTP(w, r)
		}
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		p.events.ServeHTTP(w, r)
	default:
		p.monolith.ServeHTTP(w, r)
	}
}

func (p *Proxy) routeToMovies() bool {
	if !p.gradualMigration {
		return true
	}
	return rand.Intn(100) < p.moviesPercent
}

func newReverseProxy(raw string) *httputil.ReverseProxy {
	target, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid target URL %q: %v", raw, err)
	}
	return httputil.NewSingleHostReverseProxy(target)
}

func migrationPercent() int {
	p, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil || p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
