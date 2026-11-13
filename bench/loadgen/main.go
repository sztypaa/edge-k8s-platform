package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskCFP struct {
	TaskID      string   `json:"task_id"`
	Image       string   `json:"image"`
	Command     []string `json:"command,omitempty"`
	MinCPUMilli int64    `json:"min_cpu_milli"`
	MinMemMiB   int64    `json:"min_mem_mib"`
}

type TaskResult struct {
	TaskID      string `json:"task_id"`
	ClusterID   string `json:"cluster_id"`
	Status      string `json:"status"`
	DurationMs  int64  `json:"duration_ms"`
	ImageCached bool   `json:"image_cached"`
}

type Record struct {
	Label       string
	TaskID      string
	Image       string
	SubmittedAt time.Time
	TotalMs     int64
	HTTPStatus  int
	Result      TaskResult
	Error       string
}

var csvHeader = []string{
	"label", "task_id", "image", "submitted_at", "total_ms", "dispatch_ms", "pod_ms",
	"http_status", "cluster", "status", "image_cached", "error",
}

func (r Record) row() []string {
	var dispatchMs int64
	if r.Result.ClusterID != "" {
		dispatchMs = r.TotalMs - r.Result.DurationMs
	}
	return []string{
		r.Label,
		r.TaskID,
		r.Image,
		r.SubmittedAt.Format(time.RFC3339Nano),
		strconv.FormatInt(r.TotalMs, 10),
		strconv.FormatInt(dispatchMs, 10),
		strconv.FormatInt(r.Result.DurationMs, 10),
		strconv.Itoa(r.HTTPStatus),
		r.Result.ClusterID,
		r.Result.Status,
		strconv.FormatBool(r.Result.ImageCached),
		r.Error,
	}
}

func zipfWeights(n int, alpha float64) []float64 {
	weights := make([]float64, n)
	var sum float64
	for k := 1; k <= n; k++ {
		weights[k-1] = 1 / math.Pow(float64(k), alpha)
		sum += weights[k-1]
	}
	for i := range weights {
		weights[i] /= sum
	}
	return weights
}

func pick(rng *rand.Rand, items []string, weights []float64) string {
	x := rng.Float64()
	for i, w := range weights {
		if x < w {
			return items[i]
		}
		x -= w
	}
	return items[len(items)-1]
}

func submit(client *http.Client, url string, task TaskCFP, label string) Record {
	rec := Record{Label: label, TaskID: task.TaskID, Image: task.Image, SubmittedAt: time.Now()}
	body, _ := json.Marshal(task)

	resp, err := client.Post(url+"/submit", "application/json", bytes.NewReader(body))
	rec.TotalMs = time.Since(rec.SubmittedAt).Milliseconds()
	if err != nil {
		rec.Error = err.Error()
		return rec
	}
	defer resp.Body.Close()
	rec.HTTPStatus = resp.StatusCode

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		rec.Error = strings.TrimSpace(string(data))
		return rec
	}
	if err := json.Unmarshal(data, &rec.Result); err != nil {
		rec.Error = "invalid response: " + err.Error()
	}
	return rec
}

func writeCSV(path string, records []Record) error {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if os.IsNotExist(statErr) {
		if err := w.Write(csvHeader); err != nil {
			return err
		}
	}
	for _, r := range records {
		if err := w.Write(r.row()); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func printSummary(records []Record) {
	var ok, cached int
	var times []int64
	clusters := make(map[string]int)
	for _, r := range records {
		if r.HTTPStatus == http.StatusOK && r.Result.Status == "Succeeded" {
			ok++
			times = append(times, r.TotalMs)
			clusters[r.Result.ClusterID]++
			if r.Result.ImageCached {
				cached++
			}
		}
	}
	log.Printf("[SUMMARY] Tasks: %d | Succeeded: %d | Failed/rejected: %d | Image cached: %d", len(records), ok, len(records)-ok, cached)
	if len(times) > 0 {
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		var sum int64
		for _, t := range times {
			sum += t
		}
		p95 := times[int(math.Ceil(0.95*float64(len(times))))-1]
		log.Printf("[SUMMARY] Total time: mean %dms | median %dms | p95 %dms", sum/int64(len(times)), times[len(times)/2], p95)
	}
	log.Printf("[SUMMARY] Tasks per cluster: %v", clusters)
}

func main() {
	dispatcher := flag.String("dispatcher", "http://127.0.0.1:8080", "dispatcher address")
	tasks := flag.Int("tasks", 20, "number of tasks to submit")
	rate := flag.Float64("rate", 0.5, "average number of tasks per second (Poisson arrivals)")
	burst := flag.Bool("burst", false, "submit all tasks at once instead of Poisson arrivals")
	images := flag.String("images", "busybox:latest", "comma separated images, first is the most popular")
	alpha := flag.Float64("zipf", 0.75, "Zipf exponent for choosing images")
	command := flag.String("cmd", "", "command run in the container, e.g. \"/edge-task --cpu 2s\"")
	cpu := flag.Int64("cpu", 300, "min_cpu_milli of every task")
	mem := flag.Int64("mem", 128, "min_mem_mib of every task")
	seed := flag.Int64("seed", 1, "random seed (same seed = same sequence of tasks)")
	label := flag.String("label", "run", "label written to the CSV, e.g. strategy name")
	out := flag.String("out", "results.csv", "CSV file, rows are appended")
	flag.Parse()

	imageList := strings.Split(*images, ",")
	weights := zipfWeights(len(imageList), *alpha)
	rng := rand.New(rand.NewSource(*seed))
	client := &http.Client{Timeout: 10 * time.Minute}

	var cmd []string
	if *command != "" {
		cmd = strings.Fields(*command)
	}

	log.Printf("[INFO] Submitting %d tasks to %s (label: %s, burst: %v, rate: %.2f/s)", *tasks, *dispatcher, *label, *burst, *rate)

	records := make([]Record, *tasks)
	var wg sync.WaitGroup
	runID := time.Now().Unix()
	for i := 0; i < *tasks; i++ {
		task := TaskCFP{
			TaskID:      fmt.Sprintf("%s-%d-%d", *label, runID, i),
			Image:       pick(rng, imageList, weights),
			Command:     cmd,
			MinCPUMilli: *cpu,
			MinMemMiB:   *mem,
		}

		wg.Add(1)
		go func(i int, task TaskCFP) {
			defer wg.Done()
			records[i] = submit(client, *dispatcher, task, *label)
			r := records[i]
			log.Printf("[DONE] %s | %s | HTTP %d | %s on %s | total %dms | cached %v %s",
				r.TaskID, r.Image, r.HTTPStatus, r.Result.Status, r.Result.ClusterID, r.TotalMs, r.Result.ImageCached, r.Error)
		}(i, task)

		if !*burst && i < *tasks-1 {
			time.Sleep(time.Duration(rng.ExpFloat64() / *rate * float64(time.Second)))
		}
	}
	wg.Wait()

	if err := writeCSV(*out, records); err != nil {
		log.Fatalf("[FATAL] Failed to write %s: %v", *out, err)
	}
	printSummary(records)
	log.Printf("[INFO] Results appended to %s", *out)
}
