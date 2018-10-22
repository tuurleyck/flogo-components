package elasticloud

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/elastic/beats/libbeat/outputs/elasticsearch"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/pkg/errors"
)

// ActivityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-elasticloud")

const (
	defaultCloudPort = "443"

	methodIndex  = "Index"
	methodIngest = "Ingest"

	ivCloudID    = "cloud_id"
	ivCloudAuth  = "cloud_auth"
	ivIndex      = "index"
	ivParams     = "params"
	ivDocumentID = "document_id"
	ivDocument   = "value"

	ovOutput = "response"
)

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)

	logger.Debug("START Elasticloud Activity")
}

/*
Integration with Elasticsearch
inputs: {cloud_id, cloud_auth, index, params, id, document}
outputs: {response}
*/
type ElasticloudActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new activity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &ElasticloudActivity{metadata: metadata}
}

// Metadata implements activity.Activity.Metadata
func (a *ElasticloudActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements activity.Activity.Eval - Elasticsearch integration
func (a *ElasticloudActivity) Eval(context activity.Context) (done bool, err error) {

	cloudID, _ := context.GetInput(ivCloudID).(string)
	cloudAuth, _ := context.GetInput(ivCloudAuth).(string)
	index, _ := context.GetInput(ivIndex).(string)
	params := context.GetInput(ivParams).(map[string]string)
	id := context.GetInput(ivDocumentID).(string)
	document := context.GetInput(ivDocument)

	// Decode URI & Authentication
	esURI, _, _ := decodeCloudID(cloudID)
	username, password, _ := decodeCloudAuth(cloudAuth)
	logger.Debug("esURI: ", esURI)

	// Create elasticsearch client
	logger.Debug("Build new client ..")
	client, err := elasticsearch.NewClient(elasticsearch.ClientSettings{
		URL:   esURI,
		Index: outil.MakeSelector(),
		//Headers:          headers,
		Username:         username,
		Password:         password,
		Timeout:          60 * time.Second,
		CompressionLevel: 3,
	}, nil)

	// Index document
	logger.Debug("Index document (it's a POST, automatic id creation) ..")
	responseCode, resp, err := client.Index(index, "_doc", id, params, document)
	if err != nil {
		activityLog.Errorf("Index error: %v", err)
		return false, err
	}

	context.SetOutput(ovOutput, resp)
	logger.Debug("Result: ", responseCode, " - ", resp, " and error: ", err)

	return true, nil
}

// decodeCloudID decodes the cloud.id into elasticsearch-URL and kibana-URL
func decodeCloudID(cloudID string) (string, string, error) {

	// 1. Ignore anything before `:`.
	idx := strings.LastIndex(cloudID, ":")
	if idx >= 0 {
		cloudID = cloudID[idx+1:]
	}

	// 2. base64 decode
	decoded, err := base64.StdEncoding.DecodeString(cloudID)
	if err != nil {
		return "", "", errors.Wrapf(err, "base64 decoding failed on %s", cloudID)
	}

	// 3. separate based on `$`
	words := strings.Split(string(decoded), "$")
	if len(words) < 3 {
		return "", "", errors.Errorf("Expected at least 3 parts in %s", string(decoded))
	}

	// 4. extract port from the ES and Kibana host, or use 443 as the default
	host, port := extractPortFromName(words[0], defaultCloudPort)
	esID, esPort := extractPortFromName(words[1], port)
	kbID, kbPort := extractPortFromName(words[2], port)

	// 5. form the URLs
	esURL := url.URL{Scheme: "https", Host: fmt.Sprintf("%s.%s:%s", esID, host, esPort)}
	kibanaURL := url.URL{Scheme: "https", Host: fmt.Sprintf("%s.%s:%s", kbID, host, kbPort)}

	return esURL.String(), kibanaURL.String(), nil
}

// decodeCloudAuth splits the cloud.auth into username and password.
func decodeCloudAuth(cloudAuth string) (string, string, error) {

	idx := strings.Index(cloudAuth, ":")
	if idx < 0 {
		return "", "", errors.New("cloud.auth setting doesn't contain `:` to split between username and password")
	}

	return cloudAuth[0:idx], cloudAuth[idx+1:], nil
}

// extractPortFromName takes a string in the form `id:port` and returns the
// ID and the port. If there's no `:`, the default port is returned
func extractPortFromName(word string, defaultPort string) (id, port string) {
	idx := strings.LastIndex(word, ":")
	if idx >= 0 {
		return word[:idx], word[idx+1:]
	}
	return word, defaultPort
}
