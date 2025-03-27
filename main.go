// export GOMAXPROCS=100

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Result struct {
	start   time.Time
	end     time.Time
	objects int
	size    uint64
	rate    float64
}

type ChannelSample struct {
	size       uint64
	objectName string
}

func downloadFile(client *minio.Client, ctx context.Context, bucketName string, key string, channel chan<- ChannelSample) {

	reader, err := client.GetObject(ctx, bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()
	// Save result to io.Discard this is like writing to /dev/null
	// it is convenient to get the size of the object and simulate a write
	// but not be bound by the disk speed.
	isize, err := io.Copy(io.Discard, reader)
	if err != nil {
		log.Printf("Failed to read data for %s: %v", key, err)
		return
	}

	size := uint64(isize)
	channel <- ChannelSample{size: size, objectName: key}
}

func collectResult(channel chan ChannelSample, task_counter *int, result *Result) {
	count := 0
	var total_size uint64 = 0
	total_duration := 0.0
	rate := 0.0
	startTime := time.Now()
	lastPrint := time.Now()
	for s := range channel {
		count++
		total_size += s.size
		total_duration = time.Since(startTime).Seconds()
		rate = float64(total_size) / total_duration
		if time.Since(lastPrint) > 2*time.Second {
			fmt.Printf("Tasks ~%d. Downloaded %d files: Total %s in %v at a rate of %s/s (%s). Object name: %s (%s)\n",
				*task_counter,
				count,
				humanize.Bytes(total_size),
				time.Duration(total_duration*float64(time.Second)).Round(time.Millisecond),
				humanize.Bytes(uint64(rate)),
				humanize.SIWithDigits(8*rate, 2, "b/s"),
				s.objectName,
				humanize.Bytes(uint64(s.size)),
			)
			lastPrint = time.Now()
		}
	}
	total_duration = time.Since(startTime).Seconds()
	rate = float64(total_size) / total_duration
	result.start = startTime
	result.end = time.Now()
	result.objects = count
	result.size = total_size
	result.rate = rate
}
func displayResults(result *Result, config *Config) {
	fmt.Printf("\nTotal downloaded %d objects, %s in %v at a rate of %s/s (%s)\n",
		result.objects,
		humanize.Bytes(uint64(result.size)),
		time.Duration(result.end.Sub(result.start).Seconds()*float64(time.Second)),
		humanize.Bytes(uint64(result.rate)),
		humanize.SIWithDigits(8*result.rate, 2, "b/s"))

	Hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	type Response struct {
		Host               string  `json:"host"`
		S3host             string  `json:"s3host"`
		Https              bool    `json:"https"`
		Start_time         int64   `json:"start_time"`
		End_time           int64   `json:"end_time"`
		Duration           int64   `json:"duration"`
		Bucket_name        string  `json:"bucket_name"`
		Downloaded_bytes   uint64  `json:"downloaded_bytes"`
		Downloaded_objects uint64  `json:"downloaded_objects"`
		Download_rate      float64 `json:"download_rate"`
	}
	response := Response{
		Host:               Hostname,
		S3host:             config.Host,
		Https:              config.UseSSL,
		Start_time:         result.start.Unix(),
		End_time:           result.end.Unix(),
		Duration:           result.end.Unix() - result.start.Unix(),
		Bucket_name:        config.BucketName,
		Downloaded_bytes:   result.size,
		Downloaded_objects: uint64(result.objects),
		Download_rate:      result.rate,
	}
	data, err := json.Marshal(response)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
	fmt.Println(string(data))
}
func walkBucketObjects(config Config, samples chan<- ChannelSample, counter *int) {

	ctx := context.TODO()
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   60 * time.Second,
			KeepAlive: 55 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   60 * time.Second,
		ExpectContinueTimeout: 60 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	}
	s3Client, err := minio.New(config.Host,
		&minio.Options{
			Creds:     credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
			Secure:    config.UseSSL,
			Transport: tr,
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	opts := minio.ListObjectsOptions{
		Recursive:       true,
		ReverseVersions: false,
		WithVersions:    false,
		WithMetadata:    false,
		MaxKeys:         3000, // <1000 Causes more fetches but reduces memory usage.
	}

	var wg sync.WaitGroup
	// List all objects in a bucket.
	max_counter := 0
	maxSecondsStart := time.Now()
	for object := range s3Client.ListObjects(ctx, config.BucketName, opts) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			*counter++ // Counter is updated from multiple goroutines with no Locks or synchronization, expect inconsistency.
			downloadFile(s3Client, ctx, config.BucketName, object.Key, samples)
			*counter--
		}()
		max_counter++
		if config.MaxObjects > 0 && max_counter >= config.MaxObjects {
			log.Println("Max objects to download reached. Please wait for the objects to be downloaded.")
			break
		}
		if config.MaxSeconds > 0 && time.Since(maxSecondsStart) >= time.Duration(config.MaxSeconds) {
			log.Println("Max seconds reached.")
			break
		}
	}

	fmt.Printf("Waiting for download to complete %d out of %d objects downloaded.\n", counter, max_counter)
	fmt.Println("There are no fancy progress indicators, please wait for download to complete.")
	wg.Wait()
	fmt.Println("Downloads done.")
}

func main() {
	config := getConfig()
	result := Result{}
	samples := make(chan ChannelSample)
	shared_counter := 0
	var wg_ctr sync.WaitGroup
	wg_ctr.Add(1)
	go func() {
		defer wg_ctr.Done()
		collectResult(samples, &shared_counter, &result)
	}()
	// Iterate through the objects and download each one asynchronously
	walkBucketObjects(config, samples, &shared_counter)
	close(samples)
	fmt.Printf("Stopping.")
	wg_ctr.Wait()
	displayResults(&result, &config)
	return
}
