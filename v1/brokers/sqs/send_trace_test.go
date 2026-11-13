package sqs

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
)

func TestSendMessageSpan(t *testing.T) {
	tracer := mocktracer.New()
	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	t.Cleanup(func() { opentracing.SetGlobalTracer(old) })
	for _, status := range []int{http.StatusOK, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			tracer.Reset()
			sig := &tasks.Signature{Name: "test", UUID: "fixed", RoutingKey: "queue"}
			body, err := json.Marshal(sig)
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, r.ParseForm())
				require.Equal(t, string(body), r.Form.Get("MessageBody"))
				time.Sleep(time.Millisecond)
				w.Header().Set("Content-Type", "text/xml")
				w.WriteHeader(status)
				if status != http.StatusOK {
					fmt.Fprint(w, `<ErrorResponse><Error><Code>AccessDenied</Code><Message>denied</Message></Error></ErrorResponse>`)
					return
				}
				fmt.Fprintf(w, `<SendMessageResponse><SendMessageResult><MD5OfMessageBody>%x</MD5OfMessageBody><MessageId>id</MessageId></SendMessageResult></SendMessageResponse>`, md5.Sum(body))
			}))
			defer server.Close()
			sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), Credentials: credentials.NewStaticCredentials("test", "test", ""), HTTPClient: server.Client(), MaxRetries: aws.Int(0)})
			require.NoError(t, err)
			broker := &Broker{Broker: common.NewBroker(&config.Config{Broker: server.URL}), service: awssqs.New(sess)}
			parent := tracer.StartSpan("SendTask").(*mocktracer.MockSpan)
			defer parent.Finish()
			err = broker.Publish(opentracing.ContextWithSpan(t.Context(), parent), sig)
			require.Equal(t, status != http.StatusOK, err != nil)
			spans := tracer.FinishedSpans()
			require.Len(t, spans, 1)
			require.Equal(t, "sqs.SendMessage", spans[0].OperationName)
			require.Equal(t, parent.SpanContext.SpanID, spans[0].ParentID)
			require.Equal(t, status != http.StatusOK, spans[0].Tag("error"))
			require.GreaterOrEqual(t, spans[0].FinishTime.Sub(spans[0].StartTime), time.Millisecond)
		})
	}
}
