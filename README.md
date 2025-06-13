# s3tonowhere

`s3tonowhere` is a high-performance diagnostic tool developed by **Tsolo** to measure the **network throughput** of S3-compatible object storage systems. It downloads objects directly from an S3 bucket into memory, bypassing local disk to eliminate I/O latency. This provides an accurate assessment of **pure network performance**.

Downloaded objects are **discarded on the fly** to minimize memory usage, allowing efficient operation even with large files or extended runs.

---

## Features

* Download S3 objects directly into memory (no disk writes)
* Focus purely on S3 network performance, independent of disk I/O
* Compatible with `rclone`-style configuration files
* Works with custom S3-compatible endpoints
* Memory-efficient: discards object data after download
* Prints detailed **JSON report** with throughput and size stats at the end of each run

---

## Dependencies

* `make`
* `go` (Golang)

---

## Building

To build the binary:

```bash
make
```

This will create the `s3tonowhere` binary in the `dist/` directory.

---

## First Run

```bash
dist/s3tonowhere -h
```

---

## Example Usage

```bash
dist/s3tonowhere --bucket my-bucket \
                 --rclone-config ~/.config/rclone/rclone.conf \
                 --section my-s3-end-point \
                 --limit-objects 100 \
                 fetch
```

---

## Commands

### `fetch`

Fetch objects from the specified S3 storage into memory for benchmarking.

### `show`

Print the parsed configuration to verify setup.

---

## Global Options

| Option                   | Description                                                                                                    |
| ------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `--rclone-config string` | Path to an rclone-style configuration file. Requires `--section`.                                              |
| `--section string`       | Section name to use from the config file.                                                                      |
| `--bucket string`        | Name of the S3 bucket to access.                                                                               |
| `--access-key string`    | S3 access key (if not using config file).                                                                      |
| `--secret-key string`    | S3 secret key (if not using config file).                                                                      |
| `--endpoint string`      | S3-compatible server address.                                                                                  |
| `--limit-duration int`   | Time in seconds to start new downloads. Ongoing downloads may continue beyond this. (Default: -1 for no limit) |
| `--limit-objects int`    | Limit the number of objects to fetch. (Default: -1 for no limit)                                               |
| `--quiet`, `-q`          | Reduce the amount of output.                                                                                   |
| `--debug`, `-d`          | Enable verbose debug output.                                                                                   |
| `--help`, `-h`           | Show help message.                                                                                             |

---

## Output

At the end of each run, `s3tonowhere` outputs a **JSON summary report** to `stdout`. This includes:

* Download duration and timing
* Host and endpoint information
* Total bytes and objects downloaded
* Throughput statistics (mean, p95, p99, etc.)
* Object size distribution
* HTTP status codes observed

This structured output makes it easy to log, parse, or integrate with other benchmarking and monitoring tools.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contributing

Contributions are welcome! Feel free to fork the repository and open a pull request. Bug reports and feature suggestions are also appreciated.
