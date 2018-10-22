package elasticloud

import (
	"io/ioutil"
	"testing"

	"github.com/TIBCOSoftware/flogo-contrib/action/flow/test"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

const (
	testCloudID    = "demo:ZXVyb3BlLXdlc3QxLmdjcC5jbG91ZC5lcy5pbyQ0MWY3YjUwOWZiMzM0NzJjODNiYjdmNzYyZDlmYTQ4NyQzOWQ5OGIzODhlMTk0MDI1ODg4NTU4NzgwZWFmMjY2Yg=="
	testCloudAuth  = "arthur:test123"
	testIndex      = "test"
	testDocumentID = ""
)

var activityMetadata *activity.Metadata

func getActivityMetadata() *activity.Metadata {

	if activityMetadata == nil {
		jsonMetadataBytes, err := ioutil.ReadFile("activity.json")
		if err != nil {
			panic("No Json Metadata found for activity.json path")
		}

		activityMetadata = activity.NewMetadata(string(jsonMetadataBytes))
	}

	return activityMetadata
}

func TestCreate(t *testing.T) {

	act := NewActivity(getActivityMetadata())

	if act == nil {
		t.Error("Activity Not Created")
		t.Fail()
		return
	}
}

func TestEval(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Failed()
			t.Errorf("panic during execution: %v", r)
		}
	}()

	act := NewActivity(getActivityMetadata())
	tc := test.NewTestActivityContext(getActivityMetadata())

	//test values
	body := map[string]interface{}{
		"user":      "test",
		"post_date": "2009-11-15T14:12:12",
		"message":   "trying out",
	}

	params := map[string]string{
		"refresh": "true",
	}

	// set input
	tc.SetInput("cloud_id", testCloudID)
	tc.SetInput("cloud_auth", testCloudAuth)
	tc.SetInput("index", testIndex)
	tc.SetInput("params", params)
	tc.SetInput("document_id", testDocumentID)
	tc.SetInput("value", body)

	//setup attrs

	act.Eval(tc)

	//check result attr
	result := tc.GetOutput("result")
	logger.Debug("Inserted: ", result)
}
