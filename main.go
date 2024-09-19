package main

import (
	"fmt"
	"net"

	api "github.com/Lucas/api/v1"
	log "github.com/Lucas/log"
	"github.com/Lucas/server"
	"google.golang.org/grpc"
)

func main() { //main program function

	listener, err := net.Listen("tcp", "8080")
	if err != nil {
		fmt.Printf("Error iniciando el servidor: %v", err)
	}

	commitLog, err := log.NewLog("/tmp/commitlog", log.Config{})
	if err != nil {
		fmt.Printf("Error iniciando el commit log: %v", err)
	}

	grpcServer, err := server.NewGRPCServer(commitLog)
	if err != nil {
		fmt.Printf("Error al inicializar el servidor gRPC: %v", err)
	}

	s := grpc.NewServer()
	api.RegisterLogServer(s, grpcServer)

	fmt.Println("Servidor gRPC escuchando en el puerto 8080...")

	// Iniciar el servidor gRPC
	if err := s.Serve(listener); err != nil {
		fmt.Printf("Error al iniciar el servidor gRPC: %v", err)
	}
}
