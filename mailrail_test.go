package mailrail

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/ljosa/go-pqueue/pqueue"
	"io/ioutil"
	"os"
	"path"
	"testing"
	ttemplate "text/template"
)

func TestParseSpec(t *testing.T) {
	spec, err := parseSpec([]byte("{\"from_name\": \"ACME, Inc.\", \"from_addr\": \"acme@example.com\", \"subject\": \"add inches to your credit card\", \"html\": \"<h1>Foo</h1> <p>Bar</p>\", \"text\": \"Foo bar\", \"recipients\": [{\"name\": \"John Doe\", \"addr\": \"johndoe@example.net\"}]}"))
	if err != nil {
		t.Fatal("parseSpec", err)
	}
	if spec.FromName != "ACME, Inc." {
		t.Fatal("spec.FromName", spec.FromName)
	}
	if spec.Recipients[0].Name != "John Doe" {
		t.Fatal("spec.Recipients[0].Recipient", spec.Recipients[0].Name)
	}
}

type MockSES struct {
	nsent int
	sent  *ses.SendEmailInput
}

func (svc *MockSES) GetSendQuota(input *ses.GetSendQuotaInput) (*ses.GetSendQuotaOutput, error) {
	maxSendRate := 3.0
	return &ses.GetSendQuotaOutput{MaxSendRate: &maxSendRate}, nil
}

func (svc *MockSES) SendEmail(input *ses.SendEmailInput) (*ses.SendEmailOutput, error) {
	messageId := "foo"
	svc.nsent += 1
	svc.sent = input
	return &ses.SendEmailOutput{MessageId: &messageId}, nil
}

func makeSendEmailInput(t *testing.T, spec string, mangler Mangler) *ses.SendEmailInput {
	dir, err := ioutil.TempDir("/tmp", "mailrail_test_makesendemailinput_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := pqueue.OpenQueue(dir)
	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	j.Set("spec", []byte(spec))
	svc := MockSES{}
	processJob(&svc, j, mangler)
	return svc.sent
}

func TestTextAndHtml(t *testing.T) {
	sent := makeSendEmailInput(t, `{
            "from_name": "John Doe",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "html": "<h1>Hello, {{.pet_name}}</h1>",
            "text": "Hello, {{.pet_name}}",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "from_name": "Johnny",
              "from_addr": "johnnydoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`, DoNotMangle)
	if *sent.Message.Body.Text.Data != "Hello, Janie" {
		t.Fatal("unexpected text:", *sent.Message.Body.Text.Data)
	}
	if *sent.Message.Body.Html.Data != "<h1>Hello, Janie</h1>" {
		t.Fatal("unexpected HTML:", *sent.Message.Body.Html.Data)
	}
}

func TestTextOnly(t *testing.T) {
	sent := makeSendEmailInput(t, `{
            "from_name": "John Doe",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "text": "Hello, {{.pet_name}}",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "from_name": "Johnny",
              "from_addr": "johnnydoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`, DoNotMangle)
	if *sent.Message.Body.Text.Data != "Hello, Janie" {
		t.Fatal("unexpected text:", *sent.Message.Body.Text.Data)
	}
	if sent.Message.Body.Html.Data != nil {
		t.Fatal("unexpected HTML:", *sent.Message.Body.Html.Data)
	}
}

func TestHtmlOnly(t *testing.T) {
	sent := makeSendEmailInput(t, `{
            "from_name": "John Doe",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "html": "<h1>Hello, {{.pet_name}}</h1>",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "from_name": "Johnny",
              "from_addr": "johnnydoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`, DoNotMangle)
	if sent.Message.Body.Text.Data != nil {
		t.Fatal("unexpected HTML:", *sent.Message.Body.Text.Data)
	}
	if *sent.Message.Body.Html.Data != "<h1>Hello, Janie</h1>" {
		t.Fatal("unexpected HTML:", *sent.Message.Body.Html.Data)
	}
}

func TestSource(t *testing.T) {
	global := makeSendEmailInput(t, `{
            "from_name": "John Dø",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "html": "<h1>Hello, {{.pet_name}}</h1>",
            "text": "Hello, {{.pet_name}}",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`, DoNotMangle)
	if *global.Source != "=?utf-8?q?John_D=C3=B8?= <johndoe@example.com>" {
		t.Fatal("unexpected source:", *global.Source)
	}
	specific := makeSendEmailInput(t, `{
            "from_name": "John Doe",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "html": "<h1>Hello, {{.pet_name}}</h1>",
            "text": "Hello, {{.pet_name}}",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "from_name": "Jøhnny",
              "from_addr": "johnnydoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`, DoNotMangle)
	if *specific.Source != "=?utf-8?q?J=C3=B8hnny?= <johnnydoe@example.com>" {
		t.Fatal("unexpected source:", *specific.Source)
	}
}

func TestProcessJob(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_checkpoint_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := pqueue.OpenQueue(dir)
	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	j.Set("spec", []byte(`{
"from_name": "John Doe",
"from_addr": "johndoe@example.com",
"subject": "Hello",
"html": "<h1>Hello, {{.pet_name}}</h1>",
"text": "Hello, {{.pet_name}}",
"recipients": [{
  "name": "Jane Doe",
"addr": "janedoe@example.com",
"from_name": "Johnny",
"context": {"pet_name": "Janie"}
}]
}`))
	svc := MockSES{}
	processJob(&svc, j, DoNotMangle)
	if svc.nsent != 1 {
		t.Fatal("expected 1 message to be sent, not", svc.nsent)
	}
	if *svc.sent.Message.Body.Text.Data != "Hello, Janie" {
		t.Fatal("unexpected text:", *svc.sent.Message.Body.Text.Data)
	}
	if *svc.sent.Message.Body.Html.Data != "<h1>Hello, Janie</h1>" {
		t.Fatal("unexpected HTML:", *svc.sent.Message.Body.Html.Data)
	}
	if *svc.sent.Source != "\"Johnny\" <johndoe@example.com>" {
		t.Fatal("unexpected source:", *svc.sent.Source)
	}
}

func TestTemplateMap(t *testing.T) {
	tmpl, err := ttemplate.New("text").Parse("Hello, {{.name}}")
	if err != nil {
		t.Fatal("Template.Parse", err)
	}
	text := new(bytes.Buffer)
	var m map[string]string
	m = make(map[string]string)
	m["name"] = "Dolly"
	if err := tmpl.Execute(text, m); err != nil {
		t.Fatal("Execute", err)
	}
	if text.String() != "Hello, Dolly" {
		t.Fatal("Unexpected template result:", text.String())
	}

}

func TestFinish(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "mailrail_test_finish_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := pqueue.OpenQueue(dir)
	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	j.Set("spec", []byte(`{
"from_name": "John Doe",
"from_addr": "johndoe@example.com",
"subject": "Hello",
"html": "<h1>Hello, {{.pet_name}}</h1>",
"text": "Hello, {{.pet_name}}",
"recipients": [{
  "name": "Jane Doe",
"addr": "janedoe@example.com",
"from_name": "Johnny",
"context": {"pet_name": "Janie"}
}]
}`))
	j.Submit()
	Process(dir, UseMockSesService(&MockSES{}))
	ensureExist(t, path.Join(dir, "done", j.Basename))
}

func ensureExist(t *testing.T, filename string) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatal(filename, err)
		} else {
			t.Fatal("stat", filename)
		}
	}
}

func TestManglers(t *testing.T) {
	spec := `{
            "from_name": "John Doe",
            "from_addr": "johndoe@example.com",
            "subject": "Hello",
            "html": "<h1>Hello, {{.pet_name}}</h1>",
            "text": "Hello, {{.pet_name}}",
            "recipients": [{
              "name": "Jane Doe",
              "addr": "janedoe@example.com",
              "from_name": "Johnny",
              "from_addr": "johnnydoe@example.com",
              "context": {"pet_name": "Janie"}
            }]
          }`
	sent1 := makeSendEmailInput(t, spec, DoNotMangle)
	if *sent1.Destination.ToAddresses[0] != "janedoe@example.com" {
		t.Fatal("unexpected To: addresses with DoNotMangle:", *sent1.Destination.ToAddresses[0])
	}
	sent2 := makeSendEmailInput(t, spec, DoNotSend)
	if sent2 != nil {
		t.Fatal("sent event with DoNotSend", *sent2)
	}
	sent3 := makeSendEmailInput(t, spec, SendToMe("johndoe@example.net"))
	if *sent3.Destination.ToAddresses[0] != "johndoe@example.net" {
		t.Fatal("unexpected To: addresses with SendToMe:", *sent3.Destination.ToAddresses[0])
	}
	sent4 := makeSendEmailInput(t, spec, SendToSimulator)
	if *sent4.Destination.ToAddresses[0] != "success@simulator.amazonses.com" {
		t.Fatal("unexpected To: addresses with SendToSimulator:", *sent4.Destination.ToAddresses[0])
	}
}
