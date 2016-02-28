// Package mailrail reads mail-merge jobs from a persistent queue and
// sends them via Amazon SES.
//
// Each job contains template for the text and/or HTML versions of an
// email, as well as a list of recipients with associated
// per-recipient data to be merged into the templates. See the `Spec`
// type.
//
// Mailrail backs off in response to SES's backpressure signals in
// order to avoid exceeding the SES sending rate limits.
package mailrail

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/ljosa/go-aimdtokenbucket/aimdtokenbucket"
	"github.com/ljosa/go-pqueue/pqueue"
	htemplate "html/template"
	"log"
	"net/mail"
	"os"
	ttemplate "text/template"
	"time"
)

// Manglers allow for stubbing behavior so the system can be tested
// without sending emails to the actual recipeints. There are some
// predefined manglers (`DoNotMangle`, `DoNotSend`, `SendToSimulator`)
// and some predefined functions that return manglers (`SendToMe`,
// `UseMockSesService`). You can also make your own.
type Mangler struct {
	ShouldSend bool
	Mangle     func(addr string) string
	SesService sesService
}

// Wait forever for new jobs and process them.
func ProcessForever(queueDir string, mangler Mangler) {
	process(queueDir, foreverMode, mangler)
}

// Process a single job.
func ProcessOne(queueDir string, mangler Mangler) {
	process(queueDir, oneMode, mangler)
}

// Process jobs until there are no more jobs, then stop.
func Process(queueDir string, mangler Mangler) {
	process(queueDir, allMode, mangler)
}

type processMode int

const (
	foreverMode processMode = iota
	oneMode                 = iota
	allMode                 = iota
)

func process(queueDir string, mode processMode, mangler Mangler) {
	q, err := pqueue.OpenQueue(queueDir)
	if err != nil {
		log.Fatalf("Failed to open queue %s: %s", queueDir, err)
	}
	svc := mangler.SesService
	if svc == nil {
		svc = ses.New(session.New(), getSesConfig())
	}
	q.RescueDeadJobs()
	for {
		job, err := q.Take()
		if err != nil {
			log.Fatal("Failed to take job:", err)
		}
		if job == nil {
			if mode == foreverMode {
				time.Sleep(time.Second)
			} else {
				break
			}
		} else {
			processJob(svc, job, mangler)
		}
		if mode == oneMode {
			break
		}
	}
}

func getSesConfig() *aws.Config {
	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		log.Fatalf("You must set the AWS_DEFAULT_REGION environment variable")
	}
	return &aws.Config{Region: aws.String(region)}
}

type Recipient struct {
	Name     string            `json:"name"`
	Addr     string            `json:"addr"`
	FromName string            `json:"from_name"`
	FromAddr string            `json:"from_addr"`
	Subject  string            `json:"subject"`
	Context  map[string]string `json:"context"`
}

type Spec struct {
	FromName   string `json:"from_name"`
	FromAddr   string `json:"from_addr"`
	Subject    string `json:"subject"`
	Html       string `json:"html"`
	Text       string `json:"text"`
	Recipients []Recipient
}

type mailing struct {
	spec         Spec
	textTemplate *ttemplate.Template
	htmlTemplate *htemplate.Template
}

type sesService interface {
	GetSendQuota(*ses.GetSendQuotaInput) (*ses.GetSendQuotaOutput, error)
	SendEmail(*ses.SendEmailInput) (*ses.SendEmailOutput, error)
}

func processJob(svc sesService, job *pqueue.Job, mangler Mangler) {
	mailing, err := getMailing(job)
	if err != nil {
		log.Printf("Job %s failed: %s", job.Basename, err)
		job.Fail()
		return
	}
	if err := mailing.dryRun(mangler); err != nil {
		log.Printf("Job %s failed: %s", job.Basename, err)
		job.Fail()
		return
	}
	maxRatePerSecond, err := getMaxSendRate(svc)
	if err != nil {
		log.Printf("Job %s failed to get max send rate from SES: %s", job.Basename, err)
		job.Submit()
		return
	}
	tb := aimdtokenbucket.NewAIMDTokenBucket(maxRatePerSecond, 1, 5*time.Minute)
	defer tb.Stop()
	i, err := getCheckpoint(job)
	if err != nil {
		log.Printf("Job %s failed to get checkpoint: %s", job.Basename, err)
		job.Fail()
		return
	}
	n := len(mailing.spec.Recipients)
	for ; i < n; i++ {
		for {
			rate := <-tb.Bucket
			log.Println("Job", job.Basename, "rate for recipient", i, "is", rate)
			messageId, err := mailing.send(svc, i, mangler)
			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					if reqErr, ok := err.(awserr.RequestFailure); ok {
						log.Println("Job", job.Basename, "recipient", i, "AWS request failure. Code:", reqErr.StatusCode(), "-- Request ID:", reqErr.RequestID())
					}
					if awsErr.Code() == "Throttling" {
						log.Println("Job", job.Basename, "recipient", i, "backing off because of throttling")
						tb.Backoff()
					} else if awsErr.Code() == "ServiceUnavailable" {
						log.Println("Job", job.Basename, "recipient", i, "backing off because service is unavailable")
						tb.Backoff()
					} else {
						log.Println("Job", job.Basename, "failed because of AWS error. Code:", awsErr.Code(), "-- Message:", awsErr.Message(), "-- OrigErr:", awsErr.OrigErr())
						job.Fail()
						return
					}
				} else {
					log.Printf("Job %s failed to send message to recipient %i: %s", job.Basename, i, err)
					job.Fail()
					return
				}
			} else {
				log.Printf("Job %s sent message to recipient %d. Message-ID: %s", job.Basename, i, messageId)
				break
			}
		}
		if err := setCheckpoint(job, i+1); err != nil {
			job.Fail()
			return
		}
	}
	job.Finish()
}

func getMailing(job *pqueue.Job) (*mailing, error) {
	var mailing mailing
	specbytes, err := job.Get("spec")
	if err != nil {
		return nil, fmt.Errorf("Cannot get spec: %s", err)
	}
	mailing.spec, err = parseSpec(specbytes)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse spec: %s", err)
	}
	if mailing.spec.Text != "" {
		mailing.textTemplate, err = ttemplate.New("text").Parse(mailing.spec.Text)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse text template: %s", err)
		}
	}
	if mailing.spec.Html != "" {
		mailing.htmlTemplate, err = htemplate.New("html").Parse(mailing.spec.Html)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse html template: %s", err)
		}
	}
	return &mailing, nil
}

func parseSpec(bytes []byte) (Spec, error) {
	var spec Spec
	if err := json.Unmarshal(bytes, &spec); err != nil {
		return Spec{}, err
	}
	return spec, nil
}

func (mailing *mailing) dryRun(mangler Mangler) error {
	for i, _ := range mailing.spec.Recipients {
		_, err := mailing.computeSendEmailInput(i, mangler)
		if err != nil {
			return fmt.Errorf("Dry run failed for recipient %s: %s\n", i, err)
		}
	}
	return nil
}

func (mailing *mailing) send(svc sesService, i int, mangler Mangler) (string, error) {
	params, err := mailing.computeSendEmailInput(i, mangler)
	if err != nil {
		return "", err
	}
	if !mangler.ShouldSend {
		return "NullMangler", nil
	}
	response, err := svc.SendEmail(params)
	if err != nil {
		return "", err
	}
	return *response.MessageId, nil
}

func (mailing *mailing) computeSendEmailInput(i int, mangler Mangler) (*ses.SendEmailInput, error) {
	recipient := mailing.spec.Recipients[i]
	var textContent *ses.Content = &ses.Content{}
	if mailing.textTemplate != nil {
		textBytes := new(bytes.Buffer)
		if err := mailing.textTemplate.Execute(textBytes, recipient.Context); err != nil {
			return nil, fmt.Errorf("Failed to render text template for recipient %s: %s\n", i, err)
		}
		textContent = &ses.Content{
			Data:    aws.String(textBytes.String()),
			Charset: aws.String("UTF-8")}
	}
	var htmlContent *ses.Content = &ses.Content{}
	if mailing.htmlTemplate != nil {
		htmlBytes := new(bytes.Buffer)
		if err := mailing.htmlTemplate.Execute(htmlBytes, recipient.Context); err != nil {
			return nil, fmt.Errorf("Failed to render HTML template for recipient %s: %s\n", i, err)
		}
		htmlContent = &ses.Content{
			Data:    aws.String(htmlBytes.String()),
			Charset: aws.String("UTF-8")}
	}
	var params ses.SendEmailInput
	params.Source = aws.String(computeSource(*mailing, i))
	params.Destination = &ses.Destination{
		ToAddresses:  []*string{aws.String(mangler.Mangle(recipient.Addr))},
		CcAddresses:  []*string{},
		BccAddresses: []*string{}}
	params.Message = &ses.Message{
		Subject: &ses.Content{
			Data:    aws.String(computeSubject(*mailing, i)),
			Charset: aws.String("UTF-8")},
		Body: &ses.Body{
			Html: htmlContent,
			Text: textContent}}
	return &params, nil
}

func computeSource(mailing mailing, i int) string {
	recipient := mailing.spec.Recipients[i]
	var fromName string
	if recipient.FromName != "" {
		fromName = recipient.FromName
	} else if mailing.spec.FromName != "" {
		fromName = mailing.spec.FromName
	} else {
		fromName = ""
	}
	var fromAddr string
	if recipient.FromAddr != "" {
		fromAddr = recipient.FromAddr
	} else {
		fromAddr = mailing.spec.FromAddr
	}
	if fromAddr == "" {
		return "<>"
	} else if fromName == "" {
		return fromAddr
	} else {
		ma := mail.Address{fromName, fromAddr}
		return ma.String()
	}
}

func computeSubject(mailing mailing, i int) string {
	recipient := mailing.spec.Recipients[i]
	if recipient.Subject != "" {
		return recipient.Subject
	} else {
		return mailing.spec.Subject
	}
}

func getMaxSendRate(svc sesService) (float64, error) {
	var params *ses.GetSendQuotaInput
	resp, err := svc.GetSendQuota(params)
	if err != nil {
		return 0.0, err
	}
	return *resp.MaxSendRate, nil
}

func identityAddr(addr string) string { return addr }

func alwaysAddr(addr string) func(string) string {
	return func(_ string) string { return addr }
}

// Mangler that does not interfere with email sending.
var DoNotMangle = Mangler{ShouldSend: true, Mangle: identityAddr, SesService: nil}

// Mangler that prevents emails from being sent.
var DoNotSend = Mangler{ShouldSend: false, Mangle: identityAddr, SesService: nil}

// Returns a mangler that casues all emails to be sent to a particular address.
func SendToMe(addr string) Mangler {
	return Mangler{ShouldSend: true, Mangle: alwaysAddr(addr), SesService: nil}
}

// Mangler that causes all emails to be sent to the SES simulator.
var SendToSimulator = Mangler{ShouldSend: true, Mangle: alwaysAddr("success@simulator.amazonses.com"), SesService: nil}

// Returns a mangler that uses a mock SES service.
func UseMockSesService(ses sesService) Mangler {
	return Mangler{
		ShouldSend: true,
		Mangle:     identityAddr,
		SesService: ses}
}
