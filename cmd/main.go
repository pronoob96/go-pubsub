package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"pub/service"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var rpcServer = new(service.RPC)
	err := rpc.Register(rpcServer)
	if err != nil {
		log.Fatal("Format of service rpc isn't correct. ", err)
	}
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":4040")
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	log.Printf("Serving RPC server on port %d", 4040)
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Error serving: ", err)
	}
}
