package ui

import "net/http"

func buildServicesHandler(providers *AWSProviders) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var services []string

		if providers.Queue != nil {
			services = append(services, "sqs")
		}
		if providers.Object != nil {
			services = append(services, "s3")
		}
		if providers.Table != nil {
			services = append(services, "dynamodb")
		}
		if providers.Function != nil {
			services = append(services, "lambda")
		}
		if providers.Logs != nil {
			services = append(services, "cloudwatch-logs")
		}
		if providers.CW != nil {
			services = append(services, "cloudwatch")
		}
		if providers.Notif != nil {
			services = append(services, "sns")
		}
		if providers.IAM != nil {
			services = append(services, "iam")
		}
		if providers.Key != nil {
			services = append(services, "kms")
		}
		if providers.Secret != nil {
			services = append(services, "secretsmanager")
		}
		if providers.Param != nil {
			services = append(services, "ssm")
		}
		if providers.APIGW != nil {
			services = append(services, "apigateway")
		}
		if providers.Catalog != nil {
			services = append(services, "glue")
		}
		if providers.EMR != nil {
			services = append(services, "emr")
		}
		if providers.EMRC != nil {
			services = append(services, "emr-containers")
		}
		if providers.Events != nil {
			services = append(services, "eventbridge")
		}
		if providers.Sfn != nil {
			services = append(services, "stepfunctions")
		}

		writeJSON(w, ServicesResponse{Services: services})
	}
}
