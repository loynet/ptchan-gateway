// webhook-receiver verifies gateway webhooks and logs their safe coordinates.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loynet/ptchan-gateway/clients/go"
)

const maxWebhookBodyBytes = 1 << 20

func main() {
	secret, err := requiredEnv("PTCHAN_INTEGRATION_SECRET")
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}

	mux := http.NewServeMux()
	mux.Handle("POST /webhook", webhookHandler(secret))
	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("shutting down webhook receiver", "error", err)
		}
	}()

	slog.Info("webhook receiver listening", "address", address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serving webhook receiver", "error", err)
		os.Exit(1)
	}
}

func webhookHandler(secret string) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxWebhookBodyBytes))
		if err != nil {
			http.Error(writer, "invalid webhook body", http.StatusBadRequest)
			return
		}
		event, err := gateway.VerifyWebhookBody(
			secret,
			request.Header.Get("x-ptchan-event-id"),
			request.Header.Get("x-ptchan-timestamp"),
			request.Header.Get("x-ptchan-signature"),
			body,
			gateway.WithWebhookMaxBodyBytes(maxWebhookBodyBytes),
		)
		if err != nil {
			http.Error(writer, "invalid webhook", http.StatusBadRequest)
			return
		}

		// Deduplicate event.EventID before performing application side effects.
		slog.Info("received gateway event",
			"event_id", event.EventID,
			"kind", event.Kind,
			"board", event.Post.Board,
			"thread_id", event.Post.ThreadID,
			"post_id", event.Post.PostID,
		)
		writer.WriteHeader(http.StatusNoContent)
	})
}

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
