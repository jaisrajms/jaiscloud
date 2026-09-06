module jaiscloud

go 1.26.3

require (
	cloud.google.com/go/firestore v1.25.0
	cloud.google.com/go/iam v1.13.0
	cloud.google.com/go/kms v1.33.0
	cloud.google.com/go/pubsub/v2 v2.7.0
	cloud.google.com/go/secretmanager v1.21.0
	github.com/aws/aws-sdk-go-v2 v1.41.7
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14
	github.com/aws/aws-sdk-go-v2/service/acm v1.38.3
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.39.1
	github.com/aws/aws-sdk-go-v2/service/athena v1.57.6
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.71.9
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.63.0
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.57.0
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.73.0
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.33.24
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.60.2
	github.com/aws/aws-sdk-go-v2/service/configservice v1.62.3
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.57.1
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.14
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.297.0
	github.com/aws/aws-sdk-go-v2/service/ecr v1.57.2
	github.com/aws/aws-sdk-go-v2/service/ecs v1.77.0
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.52.0
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2 v1.54.12
	github.com/aws/aws-sdk-go-v2/service/emr v1.59.0
	github.com/aws/aws-sdk-go-v2/service/emrcontainers v1.40.17
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.45.23
	github.com/aws/aws-sdk-go-v2/service/firehose v1.42.16
	github.com/aws/aws-sdk-go-v2/service/glue v1.139.1
	github.com/aws/aws-sdk-go-v2/service/iam v1.53.7
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.43.7
	github.com/aws/aws-sdk-go-v2/service/kms v1.50.4
	github.com/aws/aws-sdk-go-v2/service/lambda v1.89.0
	github.com/aws/aws-sdk-go-v2/service/rds v1.117.1
	github.com/aws/aws-sdk-go-v2/service/redshift v1.62.7
	github.com/aws/aws-sdk-go-v2/service/resourcegroups v1.33.26
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.31.12
	github.com/aws/aws-sdk-go-v2/service/route53 v1.62.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.99.0
	github.com/aws/aws-sdk-go-v2/service/s3control v1.70.1
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.41.5
	github.com/aws/aws-sdk-go-v2/service/ses v1.34.24
	github.com/aws/aws-sdk-go-v2/service/sfn v1.40.12
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.15
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.25
	github.com/aws/aws-sdk-go-v2/service/ssm v1.68.4
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.10
	github.com/aws/smithy-go v1.25.1
	github.com/go-chi/chi/v5 v5.2.2
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.1
	github.com/prometheus/client_golang v1.23.2
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cobra v1.9.1
	github.com/spf13/viper v1.21.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/sys v0.47.0
	google.golang.org/genproto v0.0.0-20260904194346-d0f1323225a4
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260904194346-d0f1323225a4
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.36.0
	k8s.io/apimachinery v0.36.0
	k8s.io/client-go v0.36.0
	sigs.k8s.io/yaml v1.6.0
)

require (
	cloud.google.com/go/longrunning v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.19 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260825221802-da73d73af1c5 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260317180543-43fb72c5454a // indirect
	k8s.io/utils v0.0.0-20260210185600-b8788abfbbc2 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.2 // indirect
)
