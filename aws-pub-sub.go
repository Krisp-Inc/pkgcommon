package pkgcommon

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func PublishWithContext(ctx context.Context, publishInput *sns.PublishInput) error {
	awsSession, err := BuildSession()
	if err != nil {
		return err
	}
	svc := sns.New(awsSession)

	_, err = svc.PublishWithContext(ctx, publishInput)
	return err

}

// PublishMessageToSNS to AWS sns topic
func PublishMessageToSNS(topicName string, message string, msgData map[string]*sns.MessageAttributeValue) error {
	awsSession, err := BuildSession()
	if err != nil {
		return err
	}
	svc := sns.New(awsSession)

	userCreatedTopic := GetSNSArn(topicName)

	pubMessage := &sns.PublishInput{
		MessageAttributes: msgData,
		Message:           aws.String(message),
		TopicArn:          aws.String(userCreatedTopic),
	}

	_, err = svc.Publish(pubMessage)
	if err != nil {
		return err
	}

	return nil
}

// PublishMessageToSNS to AWS sns topic ARN
func PublishMessageToSNSByARN(topicArn string, message string, msgData map[string]*sns.MessageAttributeValue) error {
	awsSession, err := BuildSession()
	if err != nil {
		return err
	}
	svc := sns.New(awsSession)

	pubMessage := &sns.PublishInput{
		MessageAttributes: msgData,
		Message:           aws.String(message),
		TopicArn:          aws.String(topicArn),
	}

	_, err = svc.PublishWithContext(context.Background(), pubMessage)
	if err != nil {
		return err
	}

	return nil
}

// sqsCallTimeout bounds every SQS call so a dead TCP connection (e.g. to a deleted VPC
// endpoint) returns an error instead of blocking forever. Must exceed the longest
// WaitTimeSeconds (20). Applied per call, not on the shared session, because the same
// session also carries long S3 uploads.
var sqsCallTimeout = 30 * time.Second

// ReceiveMessages to retrieve message from  AWS sqs
func ReceiveMessages(svc *sqs.SQS, queueURL string) ([]*sqs.Message, error) {

	receiveMessagesInput := &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(10), // max 10
		WaitTimeSeconds:     aws.Int64(3),  // max 20
		VisibilityTimeout:   aws.Int64(20), // max 20
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsCallTimeout)
	defer cancel()

	receiveMessageOutput, err :=
		svc.ReceiveMessageWithContext(ctx, receiveMessagesInput)

	if err != nil {
		return nil, err
	}

	if receiveMessageOutput == nil || len(receiveMessageOutput.Messages) == 0 {
		return nil, errors.New("messages not found")
	}

	return receiveMessageOutput.Messages, nil
}

// DeleteMessage delete the message from AWS sqs
func DeleteMessage(svc *sqs.SQS, queueURL string, handle *string) error {
	delInput := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: handle,
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqsCallTimeout)
	defer cancel()

	_, err := svc.DeleteMessageWithContext(ctx, delInput)

	if err != nil {
		return err
	}

	return nil
}
