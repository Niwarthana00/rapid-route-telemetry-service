package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/yourorg/fleet-tracker-service/internal/config"
	"github.com/yourorg/fleet-tracker-service/internal/handler"
	"github.com/yourorg/fleet-tracker-service/internal/kafka"
	internalmqtt "github.com/yourorg/fleet-tracker-service/internal/mqtt"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("config load failed")
	}

	logger := buildLogger(cfg.Log)
	logger.Info().Msg("Fleet Tracker Service starting")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Kafka producer
	producer := kafka.NewProducer(cfg.Kafka, logger)
	defer producer.Close()

	// MQTT subscriber → hands events to Kafka producer
	mqttSub, err := internalmqtt.NewSubscriber(cfg.MQTT, producer.Handle, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("MQTT subscriber init failed")
	}
	if err := mqttSub.Connect(ctx); err != nil {
		logger.Fatal().Err(err).Msg("MQTT connect failed")
	}
	defer mqttSub.Disconnect()

	// HTTP server (health only)
	router := mux.NewRouter()
	httpH := handler.NewHTTPHandler(logger)
	httpH.RegisterRoutes(router)
	router.Use(loggingMiddleware(logger))

	srv := &http.Server{
		Addr:         cfg.HTTP.ListenAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info().Str("addr", cfg.HTTP.ListenAddr).Msg("HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info().Str("signal", sig.String()).Msg("Shutdown signal received")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP graceful shutdown error")
	}
	logger.Info().Msg("Fleet Tracker Service stopped cleanly")
}

func buildLogger(cfg config.LogConfig) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil { level = zerolog.InfoLevel }
	zerolog.SetGlobalLevel(level)
	if cfg.Pretty {
		return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			With().Timestamp().Logger()
	}
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func loggingMiddleware(logger zerolog.Logger) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			logger.Debug().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Dur("duration", time.Since(start)).
				Msg("HTTP request")
		})
	}
}