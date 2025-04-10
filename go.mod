module github.com/RichardKnop/machinery

go 1.15

require (
	cloud.google.com/go v0.76.0 // indirect
	cloud.google.com/go/pubsub v1.10.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.5.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue v1.0.0
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/aws/aws-sdk-go v1.47.11
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/go-redsync/redsync/v4 v4.12.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.6.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/redis/go-redis/v9 v9.5.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.9.0
	github.com/urfave/cli v1.22.5
	go.opencensus.io v0.22.6 // indirect
	golang.org/x/oauth2 v0.0.0-20210201163806-010130855d6c // indirect
	gopkg.in/yaml.v2 v2.4.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
