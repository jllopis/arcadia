package connectors

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jllopis/arcadia/connectors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var _ connectors.Connector = (*Connector)(nil)

type Connector struct {
	C                  *sqs.SQS
	url                string
	connID             string
	awsRegion          string
	accessKeyID        string
	secretAccessKey    string
	user               string
	password           string
	defaultPutOpts     *connectors.PublishOptions
	defaultConsumeOpts *connectors.SubscribeOptions
}

func New(host string) *Connector {
	a := strings.Split(host, ".")
	if len(a) < 3 {
		log.Print("Unrecognized SQS url")
		return nil
	}

	c := &Connector{
		connID:    connectors.GenID(),
		url:       host,
		awsRegion: a[1],
	}

	return c
}

func (c *Connector) Name() string {
	return c.connID
}

func (c *Connector) ID() string {
	return c.connID
}

func (c *Connector) Dial() error {
	log.Print("[SQSConnector] Dial() called")
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(c.awsRegion),
		Credentials: credentials.NewStaticCredentials(c.accessKeyID, c.secretAccessKey, ""),
	}))
	c.C = sqs.New(sess)
	if c.C == nil {
		return errors.New("cannot connect to SQS")
	}
	return nil
}

func (c *Connector) Close() error {
	return nil
}

func (c *Connector) Put(opts *connectors.PublishOptions, msg []byte) error {
	result, err := c.C.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageBody:  aws.String(string(msg)),
		Queueurl:     &c.url,
	})
	log.Printf("[SQSConnector] sent message with ID=%s", *result.MessageId)
	return err
}

func (c *Connector) Stream(opts *connectors.PublishOptions, ch chan []byte) error {
	log.Print("[SQSConnector] f=Stream() m=called")
	go func() {
		for {
			select {
			case msg := <-ch:
				c.Put(opts, msg)
			}
		}
	}()
	return nil
}

func (c *Connector) Get(opts *connectors.SubscribeOptions) []byte {
	result, err := c.C.ReceiveMessage(&sqs.ReceiveMessageInput{
		Queueurl: &c.url,
		AttributeNames: aws.StringSlice([]string{
			"SentTimestamp",
		}),
		MaxNumberOfMessages: aws.Int64(1),
		MessageAttributeNames: aws.StringSlice([]string{
			"All",
		}),
	})
	if err != nil {
		log.Printf("[SQSConnector] unable to receive message from queue %q, %v.", c.url, err)
		return nil
	}

	if len(result.Messages) == 1 {
		b := *result.Messages[0].Body
		_, err := c.C.DeleteMessage(&sqs.DeleteMessageInput{
			Queueurl:      &c.url,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})
		if err != nil {
			log.Print("[SQSConnector] error deleting message: %v", err)
		}
		return []byte(b)
	}
	return nil
}

func (c *Connector) On(opts *connectors.SubscribeOptions, f func(s []byte)) error {
	input := make(chan []byte, 2)
	err := c.Listen(opts, input)
	if err != nil {
		return errors.New("[SQSConnector] failed listening on SQS: " + err.Error())
	}
	return nil
}

func (c *Connector) Listen(opts *connectors.SubscribeOptions, ch chan []byte) error {
	go func() {
		for {
			msg, err := c.C.ReceiveMessage(&sqs.ReceiveMessageInput{
				Queueurl:            &c.url,
				MaxNumberOfMessages: aws.Int64(1),
				WaitTimeSeconds:     aws.Int64(1),
			})
			if err != nil {
				log.Print(err)
				continue
			}
			if msg == nil || len(msg.Messages) < 1 {
				continue
			}
			ch <- []byte(*msg.Messages[0].Body)
			_, err = c.C.DeleteMessage(&sqs.DeleteMessageInput{
				Queueurl:      &c.url,
				ReceiptHandle: msg.Messages[0].ReceiptHandle,
			})
			if err != nil {
				log.Print("[SQSConnector] error deleting message: %v", err)
			}
		}
	}()
	return nil
}

func (c *Connector) String() string {
	return fmt.Sprint("Type: SQSConnector")
}

func (c *Connector) GetPublishOptions() *connectors.PublishOptions {
	return c.defaultPutOpts
}

func (c *Connector) GetSubscribeOptions() *connectors.SubscribeOptions {
	return c.defaultConsumeOpts
}
