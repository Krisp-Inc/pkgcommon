// pkgcommon's init() exits the process when the working directory has no .env
// file; run these with one present (an empty file is enough).
package pkgcommon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awsv1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	sqsv1 "github.com/aws/aws-sdk-go/service/sqs"
)

// hangingSQS accepts connections and never answers, like a socket to a vanished VPC endpoint.
func hangingSQS(t *testing.T) string {
	t.Helper()
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	// Release the handlers first: Close() waits for them, and they never finish on their own.
	t.Cleanup(func() { close(stop); srv.CloseClientConnections(); srv.Close() })
	return srv.URL
}

func shortSQSTimeout(t *testing.T) {
	t.Helper()
	old := sqsCallTimeout
	sqsCallTimeout = 300 * time.Millisecond
	t.Cleanup(func() { sqsCallTimeout = old })
}

// mustReturn fails the test if call is still blocked after 5s (the bug: it blocked forever).
func mustReturn(t *testing.T, call func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a timeout error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SQS call still blocked after 5s; no deadline applied")
	}
}

func TestReceiveMessagesTimesOut(t *testing.T) {
	url := hangingSQS(t)
	shortSQSTimeout(t)
	sess := session.Must(session.NewSession(&awsv1.Config{
		Region:      awsv1.String("us-east-1"),
		Endpoint:    awsv1.String(url),
		Credentials: credentials.NewStaticCredentials("test", "test", ""),
		MaxRetries:  awsv1.Int(0),
	}))
	svc := sqsv1.New(sess)
	mustReturn(t, func() error { _, err := ReceiveMessages(svc, url+"/1/q"); return err })
	mustReturn(t, func() error { return DeleteMessage(svc, url+"/1/q", awsv1.String("h")) })
}

func TestSubscriberReceiveTimesOut(t *testing.T) {
	url := hangingSQS(t)
	shortSQSTimeout(t)
	s := &SQSSubscriber{
		client: sqs.New(sqs.Options{
			Region:           "us-east-1",
			BaseEndpoint:     awsv2.String(url),
			Credentials:      awsv2.AnonymousCredentials{},
			RetryMaxAttempts: 1,
		}),
		queueURL: awsv2.String(url + "/1/q"),
		ctx:      context.Background(),
	}
	mustReturn(t, func() error { _, err := s.receiveMessages(); return err })
}
