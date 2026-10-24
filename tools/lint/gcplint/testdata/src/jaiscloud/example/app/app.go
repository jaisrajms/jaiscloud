package main

import (
	"fmt"

	serviceapp "jaiscloud/example/service/app" // want `\[reset-registration\]`
)

func main() {
	fmt.Println(&serviceapp.Service{})
}
