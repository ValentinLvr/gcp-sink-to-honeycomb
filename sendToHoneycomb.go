package HoneycombSinkHandler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

// As we use Cloud Events (EventArc under the hood). we have to specify the function target that will process
// the events and its name (i.e: sendToHoneycomb ). Events will be pushed to the route "/"
func init() {
	functions.CloudEvent("HoneycombSinkHandler", HoneycombSinkHandler)
}

// MessagePublishedData contains the full Pub/Sub message
// See the documentation for more details:
// https://cloud.google.com/eventarc/docs/cloudevents#pubsub
type MessagePublishedData struct {
	Message      PubSubMessage
	Subscription string
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data        []byte            `json:"data"`
	MessageID   string            `json:"messageId"`
	Attributes  map[string]string `json:"attributes"`
	PublishTime time.Time         `json:"publishTime"`
	OrderingKey string            `json:"orderingKey"`
}

var msg MessagePublishedData

// HoneycombSinkHandler consumes a CloudEvent message and extracts the Pub/Sub message.
func HoneycombSinkHandler(ctx context.Context, e event.Event) error {
	honeycombDataset, err := getEnvVar("HONEYCOMB_DATASET")
	if err != nil {
		return err
	}
	honeycombAPIKey, err := getEnvVar("HONEYCOMB_API_KEY")
	if err != nil {
		return err
	}
	// ------------- READ INCOMING PUBSUB EVENT -------------
	err = readPubSubEvent(e)
	if err != nil {
		return err
	}

	// ------------- SEND PAYLOAD TO HONEYCOMB -------------
	err = sendToHoneycomb(honeycombAPIKey, honeycombDataset)
	if err != nil {
		return err
	}

	return nil
}

func readPubSubEvent(e event.Event) error {
	// populate `msg` variable with the PubSub event Data
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %w", err)
	}

	pubSubData := string(msg.Message.Data) // Automatically decoded from base64.
	pubSubMessageID := msg.Message.MessageID
	pubSubSubName := msg.Subscription

	log.Printf("PubSub Message ID: %s", pubSubMessageID)
	log.Printf("PubSub Subscription name: %s", pubSubSubName)
	log.Printf("PubSub data: %s", pubSubData)

	return nil
}

func sendToHoneycomb(key string, dataset string) error {
	url := "https://api.honeycomb.io:443/1/events/" + dataset
	// Send POST request to Honeycomb APIs
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(msg.Message.Data))
	if err != nil {
		return fmt.Errorf("error initializing honeycomb post request %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Honeycomb-Team", key)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending post request to honeycomb %w", err)
	}
	defer resp.Body.Close()

	// Read the honeycomb API's response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading honeycomb post request response %w", err)
	}
	stringBody := string(body)
	log.Printf("Honeycomb API's response: %s", stringBody)

	return nil
}

func getEnvVar(key string) (string, error) {
	value, isPresent := os.LookupEnv(key)
	if !isPresent {
		return "", fmt.Errorf("error, %s environment variable is missing", key)
	}
	return value, nil
}
