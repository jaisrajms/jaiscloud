package main

import (
	serviceapp "jaiscloud/example/service/app"
)

func RegisterResetter(any) {}

func main() {
	svc := &serviceapp.Service{}
	RegisterResetter(svc)
}
