package main

import (
	"fmt"
	"net/http"

	api "github.com/Lucas/api"
)

func main() { //main program function

	server := api.NewServer()

	fmt.Println("Iniciating server on port :8080") //Port used

	http.ListenAndServe(":8080", server) //Initialize server
}
