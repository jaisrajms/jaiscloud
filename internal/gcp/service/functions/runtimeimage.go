package functions

import "os"

// gcpRuntimeImages maps Cloud Functions runtime ids to the container images the
// shared Lambda executor runs. Cloud Functions and Lambda share no runtime
// identifiers, so an explicit table is required; each GCP runtime maps to the
// closest Lambda public RIE image. The executor's runtime contract is the
// Lambda runtime interface, so a test handler uses the Lambda handler signature
// (this is the documented consequence of reusing one executor, see
// docs/GA.md §7).
var gcpRuntimeImages = map[string]string{
	"python310": "public.ecr.aws/lambda/python:3.10",
	"python311": "public.ecr.aws/lambda/python:3.11",
	"python312": "public.ecr.aws/lambda/python:3.12",
	"python313": "public.ecr.aws/lambda/python:3.13",
	"nodejs18":  "public.ecr.aws/lambda/nodejs:18",
	"nodejs20":  "public.ecr.aws/lambda/nodejs:20",
	"nodejs22":  "public.ecr.aws/lambda/nodejs:22",
	"nodejs24":  "public.ecr.aws/lambda/nodejs:22", // no nodejs:24 RIE image yet
	"java17":    "public.ecr.aws/lambda/java:17",
	"java21":    "public.ecr.aws/lambda/java:21",
	"dotnet6":   "public.ecr.aws/lambda/dotnet:6",
	"dotnet8":   "public.ecr.aws/lambda/dotnet:8",
	"ruby32":    "public.ecr.aws/lambda/ruby:3.2",
	"ruby33":    "public.ecr.aws/lambda/ruby:3.3",
	"go121":     "public.ecr.aws/lambda/provided:al2023",
	"go122":     "public.ecr.aws/lambda/provided:al2023",
	"go123":     "public.ecr.aws/lambda/provided:al2023",
	"go124":     "public.ecr.aws/lambda/provided:al2023",
	// No PHP Lambda RIE image is published; the provided runtime is the closest
	// generic image.
	"php82": "public.ecr.aws/lambda/provided:al2023",
	"php83": "public.ecr.aws/lambda/provided:al2023",
	"php84": "public.ecr.aws/lambda/provided:al2023",
}

// RuntimeImage returns the container image the executor should run for a Cloud
// Functions runtime. An unknown runtime returns "" so the executor applies its
// own default. JAISCLOUD_FUNCTIONS_IMAGE overrides every mapping (single-image
// setups, mirroring JAISCLOUD_LAMBDA_IMAGE).
func RuntimeImage(runtime string) string {
	if v := os.Getenv("JAISCLOUD_FUNCTIONS_IMAGE"); v != "" {
		return v
	}
	return gcpRuntimeImages[runtime]
}
