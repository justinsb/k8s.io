package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"k8s.io/klog"
)

func main() {
	ctx := context.Background()

	err := run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

func run(ctx context.Context) error {
	projectID := ""
	flag.StringVar(&projectID, "project", projectID, "GCP project")

	prefix := ""
	flag.StringVar(&prefix, "prefix", prefix, "Prefix")

	bucket := ""
	flag.StringVar(&bucket, "bucket", bucket, "Bucket")

	klog.InitFlags(nil)

	flag.Parse()

	if prefix == "" {
		return fmt.Errorf("--prefix is required")
	}
	if bucket == "" {
		return fmt.Errorf("--bucket is required")
	}
	if projectID == "" {
		return fmt.Errorf("--project is required")
	}

	sink, err := NewBigQuerySink(ctx, projectID)
	if err != nil {
		return err
	}

	sess, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("error creating AWS session: %v", err)
	}

	region := "us-east-1"

	config := &aws.Config{
		Region: aws.String(region),
	}
	s3Service := s3.New(sess, config)

	p := "s3://" + bucket + "/" + prefix

	var taskErrors []error
	err = s3Service.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(p *s3.ListObjectsOutput, lastPage bool) bool {
		var tasks []Task

		// We currently get 1000 files per page, and we fetch them all simultaneously
		klog.V(2).Infof("listed %d files from S3", len(p.Contents))

		lastKey := ""
		for _, o := range p.Contents {
			uploader := &S3LogUploader{
				S3Service: s3Service,
				Bucket:    bucket,
				Object:    o,
				Sink:      sink,
			}
			tasks = append(tasks, uploader)
			lastKey = aws.StringValue(o.Key)
		}

		errors := RunTasks(ctx, tasks)
		if len(errors) != 0 {
			taskErrors = append(taskErrors, errors...)
			return false // Stop
		}
		if n, err := sink.Flush(ctx); err != nil {
			klog.Warningf("failed to write to sink: %v", err)
			taskErrors = append(taskErrors, err)
			return false
		} else {
			klog.Infof("flushed %d rows to bigquery, position=%s", n, lastKey)
		}

		return true // Keep going
	})
	if err != nil {
		return fmt.Errorf("error listing objects in %s: %v", p, err)
	}

	if len(taskErrors) != 0 {
		klog.Infof("encountered %d errors: example %v", len(taskErrors), taskErrors[0])
		return taskErrors[0]
	}

	return nil
}

type Task interface {
	Run(ctx context.Context) error
}

type S3LogUploader struct {
	S3Service *s3.S3
	Bucket    string
	Object    *s3.Object
	Sink      *BigQuerySink
}

func (u *S3LogUploader) Run(ctx context.Context) error {
	p := "s3://" + u.Bucket + "/" + aws.StringValue(u.Object.Key)
	result, err := u.S3Service.GetObjectWithContext(ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(u.Bucket),
			Key:    u.Object.Key,
		})
	if err != nil {
		//aerr, ok := err.(awserr.Error)
		//if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		//}
		return fmt.Errorf("error reading %s: %v", p, err)
	}

	// Make sure to close the body when done with it for S3 GetObject APIs or
	// will leak connections.
	defer result.Body.Close()

	data, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return fmt.Errorf("error reading body of %s: %v", p, err)
	}

	lineNum := 0
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		lineNum++
		id := fmt.Sprintf("%s:%s:%d", u.Bucket, aws.StringValue(u.Object.Key), lineNum)
		u.processLine(id, string(line))
		//		klog.Infof("%s", string(line))
	}

	//	klog.Infof("%s: %s", p, string(data))
	return nil
}

func RunTasks(ctx context.Context, tasks []Task) []error {
	var errors []error
	var mutex sync.Mutex
	var wg sync.WaitGroup

	for i := range tasks {
		task := tasks[i]
		wg.Add(1)

		go func() {
			err := task.Run(ctx)
			if err != nil {
				mutex.Lock()
				errors = append(errors, err)
				mutex.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return errors
}

func (u *S3LogUploader) processLine(id string, line string) {
	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case c == '"':
			lastQuote = c
			return false
		default:
			return c == ' ' //unicode.IsSpace(c)
		}
	}

	tokens := strings.FieldsFunc(line, f)

	//klog.Infof("line %s", line)
	//tokens := strings.Fields(line)
	//klog.Infof("tokens %d %v", len(tokens), strings.Join(tokens, " | "))
	if len(tokens) != 25 {
		klog.Warningf("cannot parse line (unexpected number of tokens = %d): %s", len(tokens), line)
		return
	}

	row := &bqRequestRow{
		ID:   id,
		IP:   tokens[4],
		Verb: tokens[7],
		Path: tokens[8],
	}

	{
		userAgent := tokens[17]
		userAgent = strings.Trim(userAgent, "\"")
		row.UserAgent = userAgent
	}

	//for i := range tokens {
	//	klog.Infof("%d => %s", i, tokens[i])
	//}

	{
		bytesSentString := tokens[12]
		if bytesSentString == "-" {
			row.BytesSent = 0
		} else {
			v, err := strconv.ParseInt(bytesSentString, 10, 64)
			if err != nil {
				klog.Warningf("cannot parse bytes %q in line %s: %v", bytesSentString, line, err)
				return
			}
			row.BytesSent = v
		}
	}

	{
		responseCodeString := tokens[10]
		v, err := strconv.ParseInt(responseCodeString, 10, 64)
		if err != nil {
			klog.Warningf("cannot parse response code %q in line %s: %v", responseCodeString, line, err)
		} else {
			row.ResponseCode = int32(v)
		}
	}

	{
		dateString := tokens[2] + " " + tokens[3]
		dateString = strings.TrimPrefix(dateString, "[")
		dateString = strings.TrimSuffix(dateString, "]")
		dateFormat := "02/Jan/2006:15:04:05 -0700"
		timestamp, err := time.Parse(dateFormat, dateString)
		if err != nil {
			klog.Warningf("cannot parse date %q in line %s: %v", dateString, line, err)
		} else {
			row.Timestamp = timestamp
		}
	}

	//klog.Infof("%+v", row)
	u.Sink.AddRequestRow(row)

	//klog.Infof("%s %s %s %d", ip, verb, path, bytesSent)
}

type BigQuerySink struct {
	mutex          sync.Mutex
	requestsBuffer []bigquery.ValueSaver
	requestsTable  *bigquery.Table
}

type bqRequestRow struct {
	ID           string    `bigquery:"-"`
	IP           string    `bigquery:"ip"`
	Verb         string    `bigquery:"verb"`
	Path         string    `bigquery:"path"`
	BytesSent    int64     `bigquery:"bytes_sent"`
	Timestamp    time.Time `bigquery:"timestamp"`
	ResponseCode int32     `bigquery:"response_code"`
	UserAgent    string    `bigquery:"user_agent"`
}

func (r *bqRequestRow) Save() (map[string]bigquery.Value, string, error) {
	insertID := r.ID

	m := map[string]bigquery.Value{
		"ip":            r.IP,
		"verb":          r.Verb,
		"path":          r.Path,
		"bytes_sent":    r.BytesSent,
		"timestamp":     r.Timestamp,
		"user_agent":    r.UserAgent,
		"response_code": r.ResponseCode,
	}

	return m, insertID, nil
}

func NewBigQuerySink(ctx context.Context, projectID string) (*BigQuerySink, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("error building bigquery client: %v", err)
	}

	dataset := client.Dataset("download_logs")
	if _, err := dataset.Metadata(ctx); err != nil {
		klog.Infof("error getting dataset metadata, trying to create: %v", err)
		if err := dataset.Create(ctx, nil); err != nil {
			return nil, fmt.Errorf("error creating dataset; %v", err)
		}
	}

	requestsTable := dataset.Table("requests")
	if _, err := requestsTable.Metadata(ctx); err != nil {
		klog.Infof("error getting table metadata, trying to create: %v", err)

		schema, err := bigquery.InferSchema(bqRequestRow{})
		if err != nil {
			return nil, fmt.Errorf("error inferring schema: %v", err)
		}
		tableMetadata := &bigquery.TableMetadata{
			Schema: schema,
			TimePartitioning: &bigquery.TimePartitioning{
				Field: "timestamp",
			},
		}
		if err := requestsTable.Create(ctx, tableMetadata); err != nil {
			return nil, fmt.Errorf("error creating table: %v", err)
		}
	}

	sink := &BigQuerySink{
		requestsTable: requestsTable,
	}

	return sink, nil
}

func (s *BigQuerySink) Flush(ctx context.Context) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	n := len(s.requestsBuffer)
	if n == 0 {
		return 0, nil
	}

	u := s.requestsTable.Uploader()
	if err := u.Put(ctx, s.requestsBuffer); err != nil {
		return 0, fmt.Errorf("error writing to bigquery: %v", err)
	}

	s.requestsBuffer = nil
	return n, nil
}

func (s *BigQuerySink) AddRequestRow(row *bqRequestRow) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.requestsBuffer = append(s.requestsBuffer, row)
}
