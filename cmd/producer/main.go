package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"processqueue/internal/task"

	"github.com/redis/go-redis/v9"
)

var client *redis.Client
var ctx = context.Background()

func connect() *redis.Client {
	url := os.Getenv("REDIS")
	opt, err := redis.ParseURL(url)
	if err != nil {
		fmt.Errorf("cant parse url")
		return nil
	}
	client := redis.NewClient(opt)
	return client

}

func posthandler(w http.ResponseWriter, r *http.Request) {
	stream := r.Body
	var task task.Task
	err := json.NewDecoder(stream).Decode(&task)
	if err != nil {
		http.Error(w, "Bad request", 500)
		return
	}
	if task.Type == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if task.Type == "send_email" {
		if task.Payload["to"] == nil || task.Payload["subject"] == nil {
			http.Error(w, "Bad request,pass to and subject fields inside the payload", http.StatusBadRequest)
			return
		}

	}
	b, err := json.Marshal(task)
	if err != nil {
		http.Error(w, "cannot convert", 500)
		return
	}
	len, err := client.RPush(ctx, "task_queue", b).Result()
	fmt.Print(len)

	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Task  '%s' has been successfully added to the queue", task.Type)

}

func main() {

	var PORT string = ":" + os.Getenv("PRODUCER_PORT")

	client = connect()
	http.HandleFunc("/add", posthandler)

	err := http.ListenAndServe(PORT, nil)

	if err != nil {
		log.Fatal("error starting the server")
	}

}
