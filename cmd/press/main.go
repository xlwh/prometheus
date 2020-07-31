package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var (
	url string
)

type PutSample struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	TimeStamp int64             `json:"timestamp,omitempty"`
	Value     float64           `json:"value,omitempty"`
}

func init() {
	flag.StringVar(&url, "url", "http://127.0.0.1:9090/api/v1/put", "Put url")
}

func main() {
	flag.Parse()
	metrics := make([]string, 0, 100)
	dc := []string{"bj", "sq", "sh", "gz"}
	contentType := "application/json;charset=utf-8"

	for i := 0; i < 100; i++ {
		metrics = append(metrics, fmt.Sprintf("test_metric_%d", i))
	}

	// 100 * 4 * 1000 = 400000 线
	// 每小时数据点：360 * 400000 = 144000000, 每小时1.44亿个数据点
	// 相当于4000个机器的上报量
	for {
		for _, metric := range metrics {
			samples := make([]PutSample, 0, 1000)
			rand.Seed(time.Now().Unix())
			for _, rg := range dc {
				for i := 0; i < 1000; i++ {
					s := PutSample{
						Metric:    metric,
						TimeStamp: time.Now().UnixNano() / 1e6,
						Value:     rand.Float64(),
					}

					tags := make(map[string]string)
					tags["region"] = rg
					tags["ns"] = fmt.Sprintf("ns-%d", i)

					s.Tags = tags

					samples = append(samples, s)
				}
			}

			b, err := json.Marshal(samples)
			if err != nil {
				log.Println("json format error:", err)
				return
			}

			body := bytes.NewBuffer(b)
			_, err = http.Post(url, contentType, body)
			if err != nil {
				log.Println("Post failed:", err)
				return
			}
		}

		time.Sleep(time.Second * 10)
	}
}
