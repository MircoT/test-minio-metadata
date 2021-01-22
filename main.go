package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func upload(minioClient *minio.Client) error {
	ctx := context.Background()

	// Make a new bucket called test.
	bucketName := "test"
	location := "eu-west-1"

	err := minioClient.MakeBucket(ctx, bucketName,
		minio.MakeBucketOptions{Region: location}, // nolint: exhaustivestruct
	)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)

			return fmt.Errorf("cannot make bucket in minio %w", err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	// Upload the file with meta data
	objectName := "sample_data.txt"
	filePath := "sample_data.txt"
	contentType := "text/plain"
	metadata := make(map[string]string)
	tags := make(map[string]string)

	metadata["insertedBy"] = "me"
	metadata["content"] = "text"
	metadata["source"] = "the world"

	tags["raw"] = "txt"

	// Upload the zip file with FPutObject
	info, err := minioClient.FPutObject(ctx,
		bucketName,
		objectName,
		filePath,
		minio.PutObjectOptions{ // nolint:exhaustivestruct
			UserMetadata: metadata,
			UserTags:     tags,
			ContentType:  contentType,
		},
	)
	if err != nil {
		log.Fatalln(err)

		return fmt.Errorf("cannot put object into minio %w", err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)

	return nil
}

type SearchType int

const (
	META SearchType = iota
	TAG
	FULL
)

type SearchOption struct {
	Type   SearchType
	Values []string
}

func search(minioClient *minio.Client, bucketName string, options SearchOption) (result []string, err error) { //nolint: funlen,gocognit,gocyclo,lll,unparam
	log.Printf("----- Search -----\n")
	log.Printf("- Options\n%+v\n-----\n", options)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	objectCh := minioClient.ListObjects(ctx, bucketName,
		minio.ListObjectsOptions{ // nolint: exhaustivestruct
			Recursive: true,
		})

	metadata := make(map[string]map[string][]string)
	tags := make(map[string]map[string][]string)

	log.Println("Bucket Objects")
	log.Println("-----")

	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)

			return nil, err
		}

		log.Printf("%+v\n", object)

		objInfo, err := minioClient.StatObject(ctx, bucketName, object.Key, minio.StatObjectOptions{})
		if err != nil {
			log.Println(err)

			return nil, fmt.Errorf("cannot get %s object stats %w", object.Key, err)
		}

		log.Println("- Metadata")

		for key, value := range objInfo.UserMetadata {
			log.Println("=>", key, ":", value)

			_, inMap := metadata[key]
			if !inMap {
				metadata[key] = make(map[string][]string)
			}

			metadata[key][value] = append(metadata[key][value], objInfo.Key)
		}

		objTags, err := minioClient.GetObjectTagging(ctx, bucketName, object.Key, minio.GetObjectTaggingOptions{})
		if err != nil {
			log.Println(err)

			return nil, fmt.Errorf("cannot get %s object tags %w", object.Key, err)
		}

		log.Println("- Tags")

		for key, value := range objTags.ToMap() {
			log.Println("=>", key, ":", value)

			_, inMap := tags[key]
			if !inMap {
				tags[key] = make(map[string][]string)
			}

			tags[key][value] = append(tags[key][value], objInfo.Key)
		}

		log.Printf("%+v\n", objInfo)
	}

	log.Println("-----")
	log.Println("Metadata collection")
	log.Printf("%+v\n", metadata)
	log.Println("-----")
	log.Println("Tags collection")
	log.Printf("%+v\n", tags)
	log.Println("-----")

	partialResults := make(map[string]int)

	switch curOption := options.Type; curOption {
	case FULL:
		fallthrough
	case TAG:
		for _, target := range options.Values {
			curTarget := strings.ToLower(target)

			for key, val := range tags {
				if strings.Contains(strings.ToLower(key), curTarget) {
					for _, file := range val {
						for _, name := range file {
							partialResults[name]++
						}
					}
				} else {
					for subKey, file := range val {
						if strings.Contains(strings.ToLower(subKey), curTarget) {
							for _, name := range file {
								partialResults[name]++
							}
						}
					}
				}
			}
		}

		if curOption != FULL {
			break
		}

		fallthrough
	case META:
		for _, target := range options.Values {
			curTarget := strings.ToLower(target)

			for key, val := range metadata {
				if strings.Contains(strings.ToLower(key), curTarget) {
					for _, file := range val {
						for _, name := range file {
							partialResults[name]++
						}
					}
				} else {
					for subKey, file := range val {
						if strings.Contains(strings.ToLower(subKey), curTarget) {
							for _, name := range file {
								partialResults[name]++
							}
						}
					}
				}
			}
		}
	}

	for key, val := range partialResults {
		if val == len(options.Values) {
			result = append(result, key)
		}
	}

	return result, nil
}

func main() { //nolint:funlen
	endpoint := "localhost:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint,
		&minio.Options{ // nolint:exhaustivestruct
			Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
			Secure: useSSL,
		})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient is now set up

	// Upload the sample file with metadata
	err = upload(minioClient)
	if err != nil {
		panic(err)
	}

	// make a simple search
	res, err := search(minioClient, "test", SearchOption{
		Type:   TAG,
		Values: []string{"raw"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result TAG: raw")
	log.Println(res)

	// make a simple search
	res, err = search(minioClient, "test", SearchOption{
		Type:   TAG,
		Values: []string{"txt"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result TAG: txt")
	log.Println(res)

	// make a simple search
	res, err = search(minioClient, "test", SearchOption{
		Type:   TAG,
		Values: []string{"raw", "txt"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result TAG: raw, txt")
	log.Println(res)

	// make a simple search
	res, err = search(minioClient, "test", SearchOption{
		Type:   META,
		Values: []string{"createdby", "me"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result META: createdby, me")
	log.Println(res)

	// make a simple search
	res, err = search(minioClient, "test", SearchOption{
		Type:   META,
		Values: []string{"insertedby", "me"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result META: insertedby, me")
	log.Println(res)

	// make a simple search
	res, err = search(minioClient, "test", SearchOption{
		Type:   FULL,
		Values: []string{"raw", "me"},
	})
	if err != nil {
		panic(err)
	}

	log.Println("Search result: raw, me")
	log.Println(res)
}
