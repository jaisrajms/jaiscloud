module localgcp-harness

go 1.26.3

require (
	cloud.google.com/go/firestore v1.25.0
	cloud.google.com/go/kms v1.33.0
	cloud.google.com/go/logging v1.13.2
	cloud.google.com/go/pubsub v1.51.1
	cloud.google.com/go/secretmanager v1.21.0
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7
	google.golang.org/genproto/googleapis/api v0.0.0-20260630182238-925bb5da69e7
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
)

require (
	cloud.google.com/go/iam v1.11.0 // indirect
	cloud.google.com/go/longrunning v1.2.0 // indirect
	cloud.google.com/go/pubsub/v2 v2.6.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
)
