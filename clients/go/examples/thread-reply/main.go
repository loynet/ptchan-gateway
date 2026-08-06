// thread-reply reads a gateway thread and can submit one reply when requested.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/loynet/ptchan-gateway/clients/go"
)

func main() {
	client, err := newClientFromEnv()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	ref, err := threadRefFromEnv()
	if err != nil {
		slog.Error("invalid thread", "error", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	thread, err := client.ReadThread(ctx, ref, 50)
	if err != nil {
		slog.Error("reading thread", "error", err)
		os.Exit(1)
	}
	slog.Info("read gateway thread", "board", thread.Board, "thread_id", thread.ThreadID, "posts", len(thread.Posts), "truncated", thread.Truncated)

	message := os.Getenv("PTCHAN_REPLY_MESSAGE")
	if message == "" {
		return
	}
	sage, err := strconv.ParseBool(defaultEnv("PTCHAN_SAGE", "false"))
	if err != nil {
		slog.Error("invalid PTCHAN_SAGE", "error", err)
		os.Exit(2)
	}
	reply, err := client.PostReply(ctx, ref, message, sage)
	if err != nil {
		slog.Error("posting reply", "error", err)
		os.Exit(1)
	}
	slog.Info("gateway reply accepted", "board", reply.Board, "thread_id", reply.ThreadID, "post_id", reply.PostID, "url", reply.URL)
}

func newClientFromEnv() (*gateway.Client, error) {
	baseURL, err := requiredEnv("PTCHAN_GATEWAY_URL")
	if err != nil {
		return nil, err
	}
	name, err := requiredEnv("PTCHAN_INTEGRATION_NAME")
	if err != nil {
		return nil, err
	}
	secret, err := requiredEnv("PTCHAN_INTEGRATION_SECRET")
	if err != nil {
		return nil, err
	}
	return gateway.New(baseURL, gateway.Credentials{Name: name, Secret: secret})
}

func threadRefFromEnv() (gateway.ThreadRef, error) {
	board, err := requiredEnv("PTCHAN_BOARD")
	if err != nil {
		return gateway.ThreadRef{}, err
	}
	threadID, err := strconv.ParseInt(os.Getenv("PTCHAN_THREAD_ID"), 10, 64)
	if err != nil || threadID <= 0 {
		return gateway.ThreadRef{}, fmt.Errorf("PTCHAN_THREAD_ID must be a positive integer")
	}
	return gateway.ThreadRef{Board: board, ThreadID: threadID}, nil
}

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func defaultEnv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
