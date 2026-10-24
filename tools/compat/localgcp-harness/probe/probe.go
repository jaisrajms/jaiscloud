package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	addr := "localhost:8081"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("DIAL ERROR: %v\n", err)
		return
	}
	defer conn.Close()

	// 1. Firestore (should be implemented)
	fs := firestorepb.NewFirestoreClient(conn)
	_, err = fs.GetDocument(ctx, &firestorepb.GetDocumentRequest{
		Name: "projects/x/databases/x/documents/x/x",
	})
	code := status.Code(err)
	fmt.Printf("Firestore.GetDocument  -> code=%v (%v)\n", code, err)

	// 2. Pub/Sub Publisher (expected Unimplemented)
	pub := pubsubpb.NewPublisherClient(conn)
	_, err = pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: "projects/x/topics/y"})
	fmt.Printf("Pubsub.GetTopic        -> code=%v (%v)\n", status.Code(err), err)

	// 3. Secret Manager (expected Unimplemented)
	sm := secretmanagerpb.NewSecretManagerServiceClient(conn)
	_, err = sm.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/x/secrets/y"})
	fmt.Printf("SecretManager.GetSecret -> code=%v (%v)\n", status.Code(err), err)

	// 4. KMS (expected Unimplemented)
	kms := kmspb.NewKeyManagementServiceClient(conn)
	_, err = kms.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: "projects/x/locations/global/keyRings/y"})
	fmt.Printf("KMS.GetKeyRing          -> code=%v (%v)\n", status.Code(err), err)

	fmt.Printf("(codes.Unimplemented=%v, codes.NotFound=%v)\n", codes.Unimplemented, codes.NotFound)
}
