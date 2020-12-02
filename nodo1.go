package main

import (
	"log"
	"net"
	"fmt"

	"github.com/Makkami/Tarea2/chat"
	"google.golang.org/grpc"
)

/*Funcion main
La logica.go solo es el servidor levandado. las funciones con relacion a la logica estan en chat.go :D
*/
func main() {
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("Failed to listen on port 9001: %v", err)
	}

	s1 := chat.Server{}

	grpcServer := grpc.NewServer()

	chat.RegisterChatServiceServer(grpcServer, &s1)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port 9001: %v", err)
	}
	fmt.Print("Escuchando")

}
