package main

import (
	"fmt"
	"net/url"
	"reflect"

	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v3"
	"gopkg.in/ini.v1"
)

type Config struct {
	S3Host        string
	S3Endpoint    string
	S3AccessKey   string
	S3SecretKey   string
	S3BucketName  string
	S3Region      string
	UseSSL        bool
	LimitDuration int
	LimitObjects  int
	Quiet         bool
	Debug         bool
}

var ARG_DEBUG string = "debug"
var ARG_QUIET string = "quiet"
var ARG_ACCESS_KEY string = "access-key"
var ARG_SECRET_KEY string = "secret-key"
var ARG_BUCKET_NAME string = "bucket"
var ARG_REGION string = "region"
var ARG_ENDPOINT string = "endpoint"
var ARG_LIMIT_DURATION string = "limit-duration"
var ARG_LIMIT_OBJECTS string = "limit-objects"

// Parse the given INI configuration file.
// It is expected that the file is in rclone format and contains a section with the given name.
func readINIConfig(config_file string, section_name string) (Config, error) {
	log.Info(fmt.Sprintf("Loading %s configuration section from %s", section_name, config_file))
	cfg, err := ini.Load(config_file)
	if err != nil {
		log.Fatal("Failed to read file. ", err)
	}
	config := Config{}
	section, err := cfg.GetSection(section_name)
	if err != nil {
		log.Fatal(err)
	}
	config.S3Region = section.Key("region").String()
	config.S3AccessKey = section.Key("access_key_id").String()
	config.S3SecretKey = section.Key("secret_access_key").String()
	config.S3Endpoint = section.Key("endpoint").String()

	endpointURL, err := url.Parse(config.S3Endpoint)
	if err != nil {
		log.Fatal(err)
	}
	config.UseSSL = endpointURL.Scheme == "https"
	config.S3Host = endpointURL.Host
	return config, err
}

// A utility function to set a string field in the Config struct.
func setConfigString(config *Config, cmd *cli.Command, field string, name string) {
	value := cmd.String(name)
	if value != "" {
		r := reflect.ValueOf(config)
		f := reflect.Indirect(r).FieldByName(field)
		if f.Kind() != reflect.Invalid {
			f.SetString(value)
		}
	}
}

// A utility function to set an integer field in the Config struct.
func setConfigInt(config *Config, cmd *cli.Command, field string, name string) {
	value := cmd.Int(name)
	if value < 1 {
		value = -1
	}
	r := reflect.ValueOf(config)
	f := reflect.Indirect(r).FieldByName(field)
	if f.Kind() != reflect.Invalid {
		f.SetInt(value)
	}
}

// Returns the Config struct used by the rest of the program
// Command line args are applied, e.g. to load config file.
func getConfig(cmd *cli.Command) *Config {
	var err error
	var config Config
	if cmd.Bool(ARG_DEBUG) {
		log.SetLevel(log.DebugLevel)
	} else if cmd.Bool(ARG_QUIET) {
		log.SetLevel(log.WarnLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	config_file := cmd.String("rclone-config")
	section := cmd.String("section")

	if config_file != "" && section != "" {
		config, err = readINIConfig(config_file, section)
	} else {
		config = Config{}
		err = nil
	}
	setConfigString(&config, cmd, "S3Region", ARG_REGION)
	setConfigString(&config, cmd, "S3AccessKey", ARG_ACCESS_KEY)
	setConfigString(&config, cmd, "S3SecretKey", ARG_SECRET_KEY)
	setConfigString(&config, cmd, "S3Endpoint", ARG_ENDPOINT)
	setConfigString(&config, cmd, "S3BucketName", ARG_BUCKET_NAME)
	setConfigInt(&config, cmd, "LimitDuration", ARG_LIMIT_DURATION)
	setConfigInt(&config, cmd, "LimitObjects", ARG_LIMIT_OBJECTS)

	if cmd.Bool(ARG_DEBUG) {
		config.Debug = true
		config.Quiet = false
	} else if cmd.Bool(ARG_QUIET) {
		config.Quiet = true
	}

	if err != nil {
		log.Fatal(err)
	}
	return &config
}
