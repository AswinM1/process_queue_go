package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"processqueue/internal/processor"
	"processqueue/internal/task"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var rc *redis.Client

var ctx = context.Background()

var jobsDone int
var total_jobs int64
var jobsFailed int
var mutex sync.Mutex

func connect() *redis.Client {

	url := os.Getenv("REDIS")

	opt, err := redis.ParseURL(url)
	if err != nil {
		log.Fatal("Cannot parse Redis URL:", err)
	}

	client := redis.NewClient(opt)

	return client
}

func delay(attempt int) time.Duration {
	wait := time.Second * time.Duration((math.Pow(2, float64(attempt)))) //for that 2pow atempt seconds
	jitter := time.Duration(rand.Int63n(int64(wait)))
	max := 30 * time.Second
	if wait > max {
		wait = max
	}
	wait += jitter
	if wait > max {
		wait = max
	}
	return wait

}
func main() {

	PORT := ":" + os.Getenv("PORT")

	rc = connect()

	var wg sync.WaitGroup

	n := 3

	for i := 0; i < n; i++ {
		wg.Add(1)

		go createWorker(rc, ctx, &wg)
	}

	http.HandleFunc("/metrics", metricsHandler)

	log.Println("Starting worker server on", PORT)

	err := http.ListenAndServe(PORT, nil)

	if err != nil {
		log.Fatal(err)
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {

	mutex.Lock()
	defer mutex.Unlock()

	var metrics task.Metrics
	metrics.Total_jobs_in_queue = int64(total_jobs)
	metrics.Jobs_done = jobsDone
	metrics.Jobs_failed = jobsFailed
	w.Header().Set("content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func createWorker(
	rc *redis.Client,
	ctx context.Context,
	wg *sync.WaitGroup,
) {

	defer wg.Done()

	for {

		job, err := rc.BLPop(
			ctx,
			0,
			"task_queue",
		).Result()

		if err != nil {
			log.Println("Error reading from Redis:", err)
			break
		}

		length, _ := rc.LLen(ctx, "task_queue").Result()
		mutex.Lock()
		total_jobs = length
		mutex.Unlock()

		var tasks task.Task

		err = json.Unmarshal(
			[]byte(job[1]),
			&tasks,
		)

		if err != nil {
			log.Println("Cannot parse task:", err)
			mutex.Lock()
			jobsFailed++
			mutex.Unlock()

			continue
		}

		err = processor.Process(tasks)

		if err != nil {
			mutex.Lock()
			jobsFailed++
			mutex.Unlock()
			tasks.Retries--

			log.Println(
				"Error processing task:",
				err,
				"Retries left:",
				tasks.Retries,
			)

			if tasks.Retries > 0 {
				tasks.Attempt++

				b, err := json.Marshal(tasks)

				if err != nil {
					log.Println("Failed to marshal retry task:", err)
					continue
				}

				time.Sleep(delay(tasks.Attempt))

				_, err = rc.RPush(
					ctx,
					"task_queue",
					b,
				).Result()

				if err != nil {
					log.Println("Failed to requeue task:", err)
				}

				continue
			}

			log.Println("Task failed after all retries")

			continue
		}
		mutex.Lock()

		jobsDone++
		mutex.Unlock()

		log.Println("Task done successfully")
	}
}
