// export GOMAXPROCS=100

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v3"
	"gonum.org/v1/gonum/stat"
)

type ChannelSample struct {
	size       uint64
	objectName string
	statusCode int
	duration   time.Duration
}

type Result struct {
	start   time.Time
	end     time.Time
	objects int
	size    uint64
	rate    float64
	samples []ChannelSample
}

type StatsSummary struct {
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	P99  float64 `json:"p99"`
	P95  float64 `json:"p95"`
	P90  float64 `json:"p90"`
	P50  float64 `json:"p50"`
	Unit string  `json:"unit"`
}

// Download one Object from S3, this function is called within a goroutine.
func downloadS3Object(client *minio.Client, ctx context.Context, bucketName string, key string, channel chan<- ChannelSample) {
	statusCode := 200
	start := time.Now()
	reader, err := client.GetObject(ctx, bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		log.Error(resp.Message, "response", resp)
		log.Fatal(err)
	}
	defer reader.Close()
	// Save result to io.Discard this is like writing to /dev/null
	// it is convenient to get the size of the object and simulate a write
	// but not be bound by the disk speed.
	isize, err := io.Copy(io.Discard, reader)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		log.Error(fmt.Sprintf("Failed to read %s:", key), "error", err)
		msg := fmt.Sprintf("%s (%d) HostID: %s RequestID: %s", resp.Code, resp.StatusCode, resp.HostID, resp.RequestID)
		log.Debug("Failed to read", err, msg)
		statusCode = resp.StatusCode
	}

	// Rather use resp.Code:str that resp.StatusCode:int for the response.
	// resp.StatusCode is an integer representing the HTTP status code of the response
	// thus it is only use full for HTTP errors, we do see other issues.
	size := uint64(isize)
	channel <- ChannelSample{size: size, objectName: key, statusCode: statusCode, duration: time.Since(start)}
}

// Listen on the Sample channel and collect all the performance stats from the goroutines doing the downloads.
func collectResult(channel chan ChannelSample, task_counter *int, result *Result) {
	count := 0
	var total_size uint64 = 0
	total_duration := 0.0
	rate := 0.0
	startTime := time.Now()
	lastPrint := time.Now()
	for sample := range channel {
		result.samples = append(result.samples, sample)
		count++
		total_size += sample.size
		total_duration = time.Since(startTime).Seconds()
		rate = float64(total_size) / total_duration
		if time.Since(lastPrint) > 2*time.Second {
			msg := fmt.Sprintf("Tasks ~%d. Downloaded %d objects: Total %s in %v.",
				*task_counter,
				count,
				humanize.Bytes(total_size),
				time.Duration(total_duration*float64(time.Second)).Round(time.Millisecond),
			)
			rate_msg := fmt.Sprintf("%s/s (%s)",
				humanize.Bytes(uint64(rate)),
				humanize.SIWithDigits(8*rate, 2, "b/s"),
			)
			log.Info(msg, "rate", rate_msg)
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

// Create a summary of the downloads.
func summariseDownloads(result *Result) (StatsSummary, StatsSummary, map[int]int) {
	var samples_rate []float64
	var samples_size []float64
	status_map := make(map[int]int)
	for _, sample := range result.samples {
		samples_size = append(samples_size, float64(sample.size))
		sample_rate := float64(sample.size) / sample.duration.Seconds()
		samples_rate = append(samples_rate, sample_rate)
		status_map[sample.statusCode]++
	}

	sort.Float64s(samples_size)
	log.Debug("object size", "max", humanize.Bytes(uint64(slices.Max(samples_size))))
	log.Debug("object size", "mean", humanize.Bytes(uint64(stat.Mean(samples_size, nil))))
	log.Debug("object size", "P99", humanize.Bytes(uint64(stat.Quantile(0.99, stat.Empirical, samples_size, nil))))
	log.Debug("object size", "P95", humanize.Bytes(uint64(stat.Quantile(0.95, stat.Empirical, samples_size, nil))))
	log.Debug("object size", "P90", humanize.Bytes(uint64(stat.Quantile(0.90, stat.Empirical, samples_size, nil))))
	log.Debug("object size", "P50", humanize.Bytes(uint64(stat.Quantile(0.50, stat.Empirical, samples_size, nil))))

	size_stats := StatsSummary{
		Max:  slices.Max(samples_size),
		Mean: stat.Mean(samples_size, nil),
		P99:  stat.Quantile(0.99, stat.Empirical, samples_size, nil),
		P95:  stat.Quantile(0.95, stat.Empirical, samples_size, nil),
		P90:  stat.Quantile(0.90, stat.Empirical, samples_size, nil),
		P50:  stat.Quantile(0.50, stat.Empirical, samples_size, nil),
		Unit: "B",
	}

	sort.Float64s(samples_rate)
	log.Debug("per object download rate", "max", humanize.Bytes(uint64(slices.Max(samples_rate))))
	log.Debug("per object download rate", "mean", humanize.Bytes(uint64(stat.Mean(samples_rate, nil))))
	log.Debug("per object download rate", "P99", humanize.Bytes(uint64(stat.Quantile(0.99, stat.Empirical, samples_rate, nil))))
	log.Debug("per object download rate", "P95", humanize.Bytes(uint64(stat.Quantile(0.95, stat.Empirical, samples_rate, nil))))
	log.Debug("per object download rate", "P90", humanize.Bytes(uint64(stat.Quantile(0.90, stat.Empirical, samples_rate, nil))))
	log.Debug("per object download rate", "P50", humanize.Bytes(uint64(stat.Quantile(0.50, stat.Empirical, samples_rate, nil))))

	rate_stats := StatsSummary{
		Max:  slices.Max(samples_rate),
		Mean: stat.Mean(samples_rate, nil),
		P99:  stat.Quantile(0.99, stat.Empirical, samples_rate, nil),
		P95:  stat.Quantile(0.95, stat.Empirical, samples_rate, nil),
		P90:  stat.Quantile(0.90, stat.Empirical, samples_rate, nil),
		P50:  stat.Quantile(0.50, stat.Empirical, samples_rate, nil),
		Unit: "B/s",
	}

	return size_stats, rate_stats, status_map
}

// After all the downloads are completed, present the results.
func displayResults(result *Result, config *Config) {
	msg := fmt.Sprintf("Downloaded %d objects, %s in %v at a rate of %s/s (%s)",
		result.objects,
		humanize.Bytes(uint64(result.size)),
		time.Duration(result.end.Sub(result.start).Seconds()*float64(time.Second)),
		humanize.Bytes(uint64(result.rate)),
		humanize.SIWithDigits(8*result.rate, 2, "b/s"))

	log.Print(msg)

	size_stats, rate_stats, status_map := summariseDownloads(result)

	Hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Initialize the Response struct to make the JSON output more readable
	type Response struct {
		Host               string       `json:"host"`
		S3host             string       `json:"s3host"`
		Https              bool         `json:"https"`
		Start_time         int64        `json:"start_time"`
		End_time           int64        `json:"end_time"`
		Bucket_name        string       `json:"bucket_name"`
		Downloaded_bytes   uint64       `json:"downloaded_bytes"`
		Downloaded_objects uint64       `json:"downloaded_objects"`
		Rate_stats         StatsSummary `json:"rate_stats"`
		Size_stats         StatsSummary `json:"size_stats"`
		Http_status        map[int]int  `json:"http_status"`
	}
	response := Response{
		Host:               Hostname,
		S3host:             config.S3Host,
		Https:              config.UseSSL,
		Start_time:         result.start.Unix(),
		End_time:           result.end.Unix(),
		Bucket_name:        config.S3BucketName,
		Downloaded_bytes:   result.size,
		Downloaded_objects: uint64(result.objects),
		Rate_stats:         rate_stats,
		Size_stats:         size_stats,
		Http_status:        status_map,
	}
	data, err := json.Marshal(response)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
	fmt.Println(string(data))
}

// Do a bucket listing (list all objects in the bucket) and start a download for each object.
// This function is where we launch the goroutines to download the objects from and the
// collectResults goroutine.
func walkBucketObjects(config *Config, samples chan<- ChannelSample, counter *int) {

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
	s3Client, err := minio.New(config.S3Host,
		&minio.Options{
			Creds:     credentials.NewStaticV4(config.S3AccessKey, config.S3SecretKey, ""),
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
	for object := range s3Client.ListObjects(ctx, config.S3BucketName, opts) {
		if object.Err != nil {
			log.Error(object.Err)
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			*counter++ // Counter is updated from multiple goroutines with no Locks or synchronization, expect inconsistency.
			if config.LimitDuration > 0 && time.Since(maxSecondsStart).Seconds() >= float64(config.LimitDuration) {
				log.Warn("Reached the limit duration. Clearing the queue.")
			} else {
				downloadS3Object(s3Client, ctx, config.S3BucketName, object.Key, samples)
			}
			*counter--
		}()
		max_counter++
		if config.LimitObjects > 0 && max_counter >= config.LimitObjects {
			log.Warn("Max objects to download reached. Please wait for the objects to be downloaded.")
			break
		}
		if config.LimitDuration > 0 && time.Since(maxSecondsStart).Seconds() >= float64(config.LimitDuration) {
			// Need a more forced way to stop the downloads in progress.
			log.Warn("Reached the limit duration. Please wait for queued downloads to complete.")
			break
		}
	}

	log.Warn("Please wait for download to complete.")
	wg.Wait()
	log.Info("Downloads done.")
}

// entry point for the fetch subcommand
func cmd_run(ctx context.Context, cmd *cli.Command) error {
	config := getConfig(cmd)

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
	log.Debug("Stopping.")
	wg_ctr.Wait()
	displayResults(&result, config)
	return nil
}

// entry point for the show subcommand
func cmd_show(ctx context.Context, cmd *cli.Command) error {
	config := getConfig(cmd)
	log.Info("Verbose logging is on")
	log.Debug("Debug logging is on")
	log.Print("Configuration:")
	v := reflect.ValueOf(*config)
	// TODO: Make this a table using bubbles from bubbletea.
	for i := range v.NumField() {
		key := v.Type().Field(i).Name
		value := v.Field(i)
		fmt.Println("\t", key, "=", value)
	}
	return nil
}

func main() {
	cmd := &cli.Command{
		Name:                  "s3tonowhere",
		Usage:                 "Download objects from the S3 storage. Objects are not written to disk.",
		Copyright:             "(c) 2025 Tsolo.io",
		EnableShellCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "rclone-config",
				Value: "",
				Usage: "Rclone configuration file path, section must also be specified",
			},
			&cli.StringFlag{
				Name:  "section",
				Value: "",
				Usage: "Section in the configuration file to use, if not specified the configuration file is not used",
			},
			&cli.StringFlag{
				Name:  ARG_BUCKET_NAME,
				Value: "",
				Usage: "S3 bucket to access",
			},
			&cli.StringFlag{
				Name:  ARG_ACCESS_KEY,
				Value: "",
				Usage: "S3 Access Key",
				// Sources: cli.EnvVars("AWS_ACCESS_KEY_ID"),
			},
			&cli.StringFlag{
				Name:  ARG_SECRET_KEY,
				Value: "",
				Usage: "S3 Secret Key",
				// Sources: cli.EnvVars("AWS_SECRET_ACCESS_KEY"),
			},
			&cli.StringFlag{
				Name:  ARG_ENDPOINT,
				Value: "",
				Usage: "S3 server address",
			},
			&cli.IntFlag{
				Name:  ARG_LIMIT_DURATION,
				Value: -1,
				Usage: "Run the test for this many seconds, only controls the starting of new downloads: the actual download time will be longer",
			},
			&cli.IntFlag{
				Name:  ARG_LIMIT_OBJECTS,
				Value: -1,
				Usage: "Download this many objects",
			},
			&cli.BoolFlag{
				Name:    ARG_QUIET,
				Aliases: []string{"q"},
				Value:   false,
				Usage:   "Reduce the output",
			},
			&cli.BoolFlag{
				Name:    ARG_DEBUG,
				Aliases: []string{"d"},
				Value:   false,
				Usage:   "Enable debug output",
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "fetch",
				Usage:  "Fetch objects from the S3 storage",
				Action: cmd_run,
			},
			{
				Name:   "show",
				Usage:  "Show the configuration",
				Action: cmd_show,
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
