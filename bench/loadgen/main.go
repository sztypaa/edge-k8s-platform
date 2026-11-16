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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s-mqtt-agent/pkg/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var reauctionSuffix = regexp.MustCompile(`-r\d+$`)

type trace struct {
	bids         []protocol.BidPayload
	dispatches   []string
	resignations int
	ackCluster   string
	ackAt        time.Time
}

type observer struct {
	mu     sync.Mutex
	traces map[string]*trace
}

func (o *observer) track(taskID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.traces[taskID] = &trace{}
}

func (o *observer) get(taskID string) *trace {
	return o.traces[reauctionSuffix.ReplaceAllString(taskID, "")]
}

func (o *observer) onBid(_ mqtt.Client, msg mqtt.Message) {
	var bid protocol.BidPayload
	if json.Unmarshal(msg.Payload(), &bid) != nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if t := o.get(bid.TaskID); t != nil {
		t.bids = append(t.bids, bid)
	}
}

func (o *observer) onExecute(_ mqtt.Client, msg mqtt.Message) {
	var task protocol.TaskCFP
	if json.Unmarshal(msg.Payload(), &task) != nil {
		return
	}
	cluster := strings.TrimPrefix(msg.Topic(), protocol.TopicExecute(""))
	o.mu.Lock()
	defer o.mu.Unlock()
	if t := o.get(task.TaskID); t != nil {
		t.dispatches = append(t.dispatches, cluster)
	}
}

func (o *observer) onAck(_ mqtt.Client, msg mqtt.Message) {
	var ack protocol.AckPayload
	if json.Unmarshal(msg.Payload(), &ack) != nil {
		return
	}
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	t := o.get(ack.TaskID)
	if t == nil {
		return
	}
	if ack.Status == protocol.AckStatusAccepted {
		t.ackCluster = ack.ClusterID
		t.ackAt = now
	} else {
		t.resignations++
	}
}

type record struct {
	label       string
	task        protocol.TaskCFP
	submittedAt time.Time
	doneAt      time.Time
	httpStatus  int
	result      protocol.TaskResult
	err         string
	trace       trace
}

var csvHeader = []string{
	"label", "task_id", "image", "submitted_at", "total_ms", "ack_ms", "pod_ms", "http_status", "status",
	"cluster", "image_cached", "bids", "dispatches", "resignations", "ack_at", "done_at", "error",
}

func (r record) row() []string {
	var ackMs int64
	ackAt := ""
	if !r.trace.ackAt.IsZero() {
		ackMs = r.trace.ackAt.Sub(r.submittedAt).Milliseconds()
		ackAt = r.trace.ackAt.Format(time.RFC3339Nano)
	}

	cached := false
	for _, bid := range r.trace.bids {
		if bid.ClusterID == r.trace.ackCluster {
			cached = bid.ImageCached
		}
	}

	return []string{
		r.label,
		r.task.TaskID,
		r.task.Image,
		r.submittedAt.Format(time.RFC3339Nano),
		strconv.FormatInt(r.doneAt.Sub(r.submittedAt).Milliseconds(), 10),
		strconv.FormatInt(ackMs, 10),
		strconv.FormatInt(r.result.DurationMs, 10),
		strconv.Itoa(r.httpStatus),
		r.result.Status,
		r.trace.ackCluster,
		strconv.FormatBool(cached),
		strconv.Itoa(len(r.trace.bids)),
		strconv.Itoa(len(r.trace.dispatches)),
		strconv.Itoa(r.trace.resignations),
		ackAt,
		r.doneAt.Format(time.RFC3339Nano),
		r.err,
	}
}

func submit(client *http.Client, url string, task protocol.TaskCFP, label string) record {
	rec := record{label: label, task: task, submittedAt: time.Now()}
	body, _ := json.Marshal(task)

	resp, err := client.Post(url+"/submit", "application/json", bytes.NewReader(body))
	rec.doneAt = time.Now()
	if err != nil {
		rec.err = err.Error()
		return rec
	}
	defer resp.Body.Close()
	rec.httpStatus = resp.StatusCode

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		rec.err = strings.TrimSpace(string(data))
		return rec
	}
	if err := json.Unmarshal(data, &rec.result); err != nil {
		rec.err = "invalid response: " + err.Error()
	}
	return rec
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

func connectMQTT(broker, user, pass string, obs *observer) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetUsername(user)
	opts.SetPassword(pass)
	opts.SetClientID(fmt.Sprintf("loadgen-%d", os.Getpid()))
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		c.Subscribe(protocol.TopicBids, protocol.MQTTQoS, obs.onBid)
		c.Subscribe(protocol.TopicAck, protocol.MQTTQoS, obs.onAck)
		c.Subscribe(protocol.TopicExecute("+"), protocol.MQTTQoS, obs.onExecute)
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return client, nil
}

func writeCSV(path string, records []record) error {
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

func main() {
	dispatcher := flag.String("dispatcher", "http://127.0.0.1:8080", "dispatcher address")
	broker := flag.String("broker", protocol.DefaultMQTTBroker, "MQTT broker address")
	user := flag.String("user", protocol.DefaultMQTTUser, "MQTT user")
	pass := flag.String("pass", protocol.DefaultMQTTPass, "MQTT password")
	tasks := flag.Int("tasks", 1, "number of tasks to submit")
	rate := flag.Float64("rate", 0.5, "average number of tasks per second (Poisson arrivals)")
	burst := flag.Bool("burst", false, "submit all tasks at once")
	images := flag.String("images", "", "comma separated images, the first one is the most popular")
	alpha := flag.Float64("zipf", 0.75, "Zipf exponent for choosing images")
	command := flag.String("cmd", "", "command run in the container, split on spaces")
	cpu := flag.Int64("cpu", protocol.DefaultTaskMinCPUMilli, "min_cpu_milli of every task")
	mem := flag.Int64("mem", protocol.DefaultTaskMinMemMiB, "min_mem_mib of every task")
	seed := flag.Int64("seed", 1, "random seed, the same seed gives the same sequence of tasks")
	label := flag.String("label", "run", "label written to the CSV")
	out := flag.String("out", "results.csv", "CSV file, rows are appended")
	flag.Parse()

	if *images == "" || *command == "" {
		log.Fatal("[FATAL] -images and -cmd are required")
	}

	obs := &observer{traces: make(map[string]*trace)}
	mqttClient, err := connectMQTT(*broker, *user, *pass, obs)
	if err != nil {
		log.Fatalf("[FATAL] MQTT connection failed: %v", err)
	}
	defer mqttClient.Disconnect(250)
	time.Sleep(500 * time.Millisecond)

	imageList := strings.Split(*images, ",")
	weights := zipfWeights(len(imageList), *alpha)
	rng := rand.New(rand.NewSource(*seed))
	httpClient := &http.Client{Timeout: 10 * time.Minute}
	cmd := strings.Fields(*command)
	runID := time.Now().Unix()

	log.Printf("[INFO] Submitting %d tasks to %s (label: %s, burst: %v)", *tasks, *dispatcher, *label, *burst)

	records := make([]record, *tasks)
	var wg sync.WaitGroup
	for i := 0; i < *tasks; i++ {
		task := protocol.TaskCFP{
			TaskID:      fmt.Sprintf("%s-%d-%d", *label, runID, i),
			Image:       pick(rng, imageList, weights),
			Command:     cmd,
			MinCPUMilli: *cpu,
			MinMemMiB:   *mem,
		}
		obs.track(task.TaskID)

		wg.Add(1)
		go func(i int, task protocol.TaskCFP) {
			defer wg.Done()
			records[i] = submit(httpClient, *dispatcher, task, *label)
			r := records[i]
			log.Printf("[DONE] %s | %s | HTTP %d | %s | %dms %s",
				task.TaskID, task.Image, r.httpStatus, r.result.Status, r.doneAt.Sub(r.submittedAt).Milliseconds(), r.err)
		}(i, task)

		if !*burst && i < *tasks-1 {
			time.Sleep(time.Duration(rng.ExpFloat64() / *rate * float64(time.Second)))
		}
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	obs.mu.Lock()
	for i := range records {
		records[i].trace = *obs.traces[records[i].task.TaskID]
	}
	obs.mu.Unlock()

	if err := writeCSV(*out, records); err != nil {
		log.Fatalf("[FATAL] Failed to write %s: %v", *out, err)
	}
	log.Printf("[INFO] Results appended to %s", *out)
}
