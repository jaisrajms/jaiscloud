package main

import functionsstore "jaiscloud/internal/gcp/store/functions"

// legacyLambdaRuntimeImages maps Cloud Functions runtime ids onto the public AWS
// Lambda RIE images the legacy (JAISCLOUD_FUNCTIONS_EXECUTOR=lambda) contract
// runs. The GCP Functions Framework profile resolves its own images; this table
// exists only for the back-compat override, which reuses the AWS Lambda
// execution contract.
var legacyLambdaRuntimeImages = map[string]string{
	"python310": "public.ecr.aws/lambda/python:3.10",
	"python311": "public.ecr.aws/lambda/python:3.11",
	"python312": "public.ecr.aws/lambda/python:3.12",
	"python313": "public.ecr.aws/lambda/python:3.13",
	"nodejs18":  "public.ecr.aws/lambda/nodejs:18",
	"nodejs20":  "public.ecr.aws/lambda/nodejs:20",
	"nodejs22":  "public.ecr.aws/lambda/nodejs:22",
	"nodejs24":  "public.ecr.aws/lambda/nodejs:22",
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
	"php82":     "public.ecr.aws/lambda/provided:al2023",
	"php83":     "public.ecr.aws/lambda/provided:al2023",
	"php84":     "public.ecr.aws/lambda/provided:al2023",
}

// legacyLambdaRuntimeImage resolves the image for the legacy Lambda contract.
// An unknown runtime returns "" so the AWS Lambda profile falls back to its own
// default.
func legacyLambdaRuntimeImage(f functionsstore.Function) string {
	return legacyLambdaRuntimeImages[f.Runtime]
}
