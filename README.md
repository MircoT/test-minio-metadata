# test-minio-metadata

A simple example of how to use MinIO metadata to search for files in a bucket using Go

## :rocket: How to run

First, start your [MinIO](https://min.io/download#), for example, using using [docker](https://www.docker.com/get-started):

```bash
docker run -p 9000:9000 minio/minio server /data
```

Then, run the program:

```bash
go run main.go
```
