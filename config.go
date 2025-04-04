package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"gopkg.in/ini.v1"
)

type Config struct {
	Host       string
	AccessKey  string
	SecretKey  string
	UseSSL     bool
	Region     string
	BucketName string
	MaxObjects int
	MaxSeconds int
}

func getConfig() Config {

	var maxObjects = flag.Int("objects", -1,
		"Limits the number of objects downloaded, a negative value means no limits. First limit reached causes application to exit.")
	// and 0 is weird behaviour.
	var maxSeconds = flag.Int("seconds", -1,
		"Limits how long the application runs for, a negative value means no limits. After this many seconds the application will start no new downloads but downloads in progress will continue until completed. First limit reached causes application to exit.")
	var bucketName = flag.String("bucket", "", "Name of the bucket to download from.")
	var sectionName = flag.String("section", "", "Name of the section from config file to use.")
	var configFileName = flag.String("config", "rclone.conf", "The name of the configuration file.")

	flag.Parse()
	// if len(os.Args) != 3 {
	// 	log.Fatalf("Usage: %s <bucket name> <section in config>", os.Args[0])
	// }
	// bucketName := os.Args[1]
	// sectionName := os.Args[2]
	if *bucketName == "" {
		log.Fatal("Bucket name is required")
	}
	if *sectionName == "" {
		log.Fatal("Section name is required")
	}

	if *maxSeconds == 0 || *maxSeconds == 0 {
		log.Fatal("Now your messing with me. Zero doesn't make sense.")
	}

	log.Println("Loading configuration from rclone.conf")
	cfg, err := ini.Load(*configFileName)
	if err != nil {
		fmt.Printf("Failed to read file: %v", err)
		os.Exit(1)
	}
	section := cfg.Section(*sectionName)
	region := section.Key("region").String()
	accessKey := section.Key("access_key_id").String()
	secretKey := section.Key("secret_access_key").String()
	endpoint := section.Key("endpoint").String()
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return Config{
		Host:       endpointURL.Host,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		UseSSL:     endpointURL.Scheme == "https",
		Region:     region,
		BucketName: *bucketName,
		MaxObjects: *maxObjects,
		MaxSeconds: *maxSeconds,
	}
}
