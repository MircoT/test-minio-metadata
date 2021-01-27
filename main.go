package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"

	"github.com/MircoT/go-string-fuzzy-finder/pkg/core"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func makeBucket(minioClient *minio.Client, ctx context.Context, bucketName string) error {
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

	return nil
}

func uploadSample(minioClient *minio.Client, ctx context.Context, bucketName string, objectName string, filePath string, metadata map[string]string, tags map[string]string, contentType string) error {
	// Upload the sample file with FPutObject
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

func upload(minioClient *minio.Client) error { //nolint:funlen
	ctx := context.Background()

	errMake := makeBucket(minioClient, ctx, "test")
	if errMake != nil {
		return fmt.Errorf("cannot make bucket test %w", errMake)
	}

	errMake = makeBucket(minioClient, ctx, "foo")
	if errMake != nil {
		return fmt.Errorf("cannot make bucket foo %w", errMake)
	}

	errMake = makeBucket(minioClient, ctx, "bar")
	if errMake != nil {
		return fmt.Errorf("cannot make bucket bar %w", errMake)
	}

	contentType := "text/plain"
	metadata := make(map[string]string)
	tags := make(map[string]string)

	// Add some metadata
	metadata["insertedBy"] = "me"
	metadata["content"] = "text"
	metadata["source"] = "the world"

	// Add a tag
	tags["raw"] = "txt"

	errUpload := uploadSample(
		minioClient,
		ctx,
		"test",
		"sample_test.txt",
		"sample_data.txt",
		metadata,
		tags,
		contentType,
	)
	if errUpload != nil {
		return fmt.Errorf("cannot upload sample test bucket %w", errUpload)
	}

	metadata["insertedBy"] = "anotherMe"
	metadata["source"] = "a tiny research"

	errUpload = uploadSample(
		minioClient,
		ctx,
		"foo",
		"sample_foo.txt",
		"sample_data.txt",
		metadata,
		tags,
		contentType,
	)
	if errUpload != nil {
		return fmt.Errorf("cannot upload sample foo bucket %w", errUpload)
	}

	metadata["insertedBy"] = "Marvin"
	metadata["source"] = "black hole"
	tags["raw"] = "plain text"

	errUpload = uploadSample(
		minioClient,
		ctx,
		"bar",
		"sample_bar.txt",
		"sample_data.txt",
		metadata,
		tags,
		contentType,
	)
	if errUpload != nil {
		return fmt.Errorf("cannot upload sample in bar bucket %w", errUpload)
	}

	return nil
}

type Catalog map[string][]string

type ResearchType int

const (
	META ResearchType = iota
	TAG
)

type fileElm struct {
	metadata string
	tags     string
}
type SearchEngine struct {
	metadata  Catalog
	tags      Catalog
	files     map[string]fileElm
	finder    core.SimpleFinder
	S3BaseURL string
}

type ResultElm struct {
	Filename string   `json:"filename"`
	Metadata string   `json:"metadata"`
	Tags     string   `json:"tags"`
	URL      string   `json:"url"`
	Match    []string `json:"match"`
}

func (s *SearchEngine) Init() {
	s.metadata = make(Catalog)
	s.tags = make(Catalog)
	s.files = make(map[string]fileElm)

	s.finder.Init()
	s.finder.SetMinThreshold(0.6)
}

func (s SearchEngine) GetPaths(allMetadata []string) []ResultElm { //nolint:funlen,gocognit
	log.Printf("%+v", allMetadata)

	elements := make([]ResultElm, 0)
	extracted := make(map[string]ResultElm)

	for _, val := range allMetadata {
		for mkey, files := range s.metadata {
			if strings.Contains(mkey, val) {
				for _, file := range files {
					curElm := ResultElm{}
					fileStats := s.files[file]
					curElm.Filename = file
					curElm.Metadata = fileStats.metadata
					curElm.Tags = fileStats.tags
					curElm.URL = fmt.Sprintf("%s/%s", s.S3BaseURL, curElm.Filename)
					curElm.Match = make([]string, 0)
					curElm.Match = append(curElm.Match, mkey)
					extracted[file] = curElm
				}
			}
		}

		for tkey, files := range s.tags {
			if strings.Contains(tkey, val) {
				for _, file := range files {
					if prevRes, inExtracted := extracted[file]; !inExtracted {
						curElm := ResultElm{}
						fileStats := s.files[file]
						curElm.Filename = file
						curElm.Metadata = fileStats.metadata
						curElm.Tags = fileStats.tags
						curElm.URL = fmt.Sprintf("%s/%s", s.S3BaseURL, curElm.Filename)
						curElm.Match = make([]string, 0)
						curElm.Match = append(curElm.Match, tkey)
						extracted[file] = curElm
					} else {
						prevRes.Match = append(prevRes.Match, tkey)
					}
				}
			}
		}
	}

	for _, obj := range extracted {
		elements = append(elements, obj)
	}

	return elements
}

func (s SearchEngine) extractFile(catType ResearchType) chan string {
	var baseMap *Catalog

	switch catType {
	case META:
		baseMap = &s.metadata
	case TAG:
		baseMap = &s.tags
	}

	ch := make(chan string)

	go func() {
		defer close(ch)

		for _, files := range *baseMap {
			for _, file := range files {
				ch <- file
			}
		}
	}()

	return ch
}

func (s SearchEngine) AvailableFiles() []string {
	allFiles := make([]string, 0)
	duplicates := make(map[string]bool)

	for file := range s.extractFile(META) {
		if _, inMap := duplicates[file]; !inMap {
			allFiles = append(allFiles, file)
			duplicates[file] = true
		}
	}

	for file := range s.extractFile(TAG) {
		if _, inMap := duplicates[file]; !inMap {
			allFiles = append(allFiles, file)
			duplicates[file] = true
		}
	}

	return allFiles
}

func (s SearchEngine) Metadata() []string {
	allMetadata := make([]string, 0)

	for key, _ := range s.metadata {
		allMetadata = append(allMetadata, key)
	}

	return allMetadata
}

func (s SearchEngine) SplitMeta(data []string) []string {
	results := make([]string, 0)

	for _, val := range data {
		results = append(results, strings.Split(val, "=")...)
	}

	return results
}

func (s SearchEngine) Tags() []string {
	allTags := make([]string, 0)

	for key, _ := range s.tags {
		allTags = append(allTags, key)
	}

	return allTags
}

func (s SearchEngine) catalog(catType ResearchType) *Catalog {
	var baseMap *Catalog

	switch catType {
	case META:
		baseMap = &s.metadata
	case TAG:
		baseMap = &s.tags
	}

	return baseMap
}

func (s *SearchEngine) Insert(catType ResearchType, fullKey string, filePath string) {
	baseMap := s.catalog(catType)

	_, inMap := (*baseMap)[fullKey]
	if !inMap {
		(*baseMap)[fullKey] = make([]string, 0)
	}

	(*baseMap)[fullKey] = append((*baseMap)[fullKey], filePath)

	if _, inMap := s.files[filePath]; !inMap {
		s.files[filePath] = fileElm{}
	}

	curValues := s.files[filePath]

	switch catType {
	case META:
		curValues.metadata = fmt.Sprintf("%s;%s", fullKey, curValues.metadata)
	case TAG:
		curValues.tags = fmt.Sprintf("%s;%s", fullKey, curValues.tags)
	}

	s.files[filePath] = curValues
}

func (s *SearchEngine) metadataCollector(minioClient *minio.Client) error {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("cannot read bucket list %w", err)
	}

	for _, bucket := range buckets {
		objectCh := minioClient.ListObjects(ctx, bucket.Name,
			minio.ListObjectsOptions{ // nolint: exhaustivestruct
				Recursive: true,
			})

		for object := range objectCh {
			if object.Err != nil {
				return fmt.Errorf("cannot read object in bucket %s: %w", bucket.Name, err)
			}

			log.Printf("%+v\n", object)

			objInfo, err := minioClient.StatObject(ctx, bucket.Name, object.Key, minio.StatObjectOptions{})
			if err != nil {
				return fmt.Errorf("cannot get stats of object %s in bucket %s: %w", object.Key, bucket.Name, err)
			}

			filePath := path.Join(bucket.Name, objInfo.Key)

			for key, value := range objInfo.UserMetadata {
				log.Println("[", filePath, "]=>", key, ":", value)

				fullKey := fmt.Sprintf("%s=%s", key, value)
				s.Insert(META, fullKey, filePath)

			}

			objTags, err := minioClient.GetObjectTagging(ctx, bucket.Name, object.Key, minio.GetObjectTaggingOptions{})
			if err != nil {
				return fmt.Errorf("cannot get tags of object %s in bucket %s: %w", object.Key, bucket.Name, err)
			}

			log.Println("- Tags")

			for key, value := range objTags.ToMap() {
				log.Println("=>", key, ":", value)

				fullKey := fmt.Sprintf("%s=%s", key, value)
				s.Insert(TAG, fullKey, filePath)
			}

			log.Printf("%+v\n", objInfo)
		}
	}

	fmt.Printf("%#v\n", s.metadata)
	fmt.Printf("%#v\n", s.tags)

	return nil
}

func (s SearchEngine) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	allMetadata := s.Metadata()
	allMetadata = append(allMetadata, s.Tags()...)
	allMetadata = append(allMetadata, s.SplitMeta(allMetadata)...)

	log.Println(allMetadata)

	log.Printf("%#v\n", req)
	log.Printf("searchString=%s", req.URL.Query().Get("searchString"))

	allResults := make([]string, 0)

	for _, word := range strings.Split(req.URL.Query().Get("searchString"), " ") {
		results, _ := s.finder.Similars(word, allMetadata)
		log.Println("results for word", word, "->", results)

		allResults = append(allResults, results...)
	}

	data := struct {
		Results []ResultElm `json:"results"`
	}{
		Results: s.GetPaths(allResults),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		res.WriteHeader(500)

		_, err = res.Write([]byte("Error on write JSON data..."))
		if err != nil {
			panic(err)
		}

		return
	}

	log.Printf("%s", string(jsonData))

	res.WriteHeader(200)

	_, err = res.Write(jsonData)
	if err != nil {
		panic(err)
	}
}

func main() {
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

	searchEngine := SearchEngine{
		S3BaseURL: "http://" + endpoint,
	}

	searchEngine.Init()

	err = searchEngine.metadataCollector(minioClient)
	if err != nil {
		panic(err)
	}

	fmt.Println("Files:", searchEngine.AvailableFiles())
	fmt.Println("Metadata:", searchEngine.Metadata())
	fmt.Println("Tags:", searchEngine.Tags())

	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.Dir("assets")))
	mux.Handle("/search", searchEngine)

	s := &http.Server{
		Addr:    "localhost:9009",
		Handler: mux,
	}

	fmt.Println("Server started at localhost:9009")

	err = s.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
