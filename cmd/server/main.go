package main

import (
	"fmt"
	"net/http"
	"time"
	"github.com/jatin2567/kvserver/internal/ApiHttpLayer"
)

func main(){
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", Handler.HandleGet)

	mux.HandleFunc("PUT /", Handler.HandlePut)

	mux.HandleFunc("DELETE /", Handler.HandleDelete)

	server := &http.Server{
		Addr: ":8080",
		Handler: mux,
		ReadTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout: 150 * time.Second,
	}

	fmt.Println("Starting custom server on http://localhost:8080")
	err := server.ListenAndServe()
	if err != nil{
		fmt.Printf("Server failed: %s\n", err)
	}
}