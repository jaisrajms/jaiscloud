package main

import (
	serviceapp "jaiscloud/example/service/app"
	missingapp "jaiscloud/example/service/missing" // want `\[reset-registration\].*has no Reset`
)

func RegisterResetter(any) {}

func main() {
	svc := &serviceapp.Service{}
	RegisterResetter(svc)
	_ = &missingapp.Service{}
}
