package main

import (
	"log"
	"net"
	"fmt"
	"context"

	"github.com/Makkami/Tarea2/chat"
	"google.golang.org/grpc"
)

/*Funcion main
La logica.go solo es el servidor levandado. las funciones con relacion a la logica estan en chat.go :D
*/
func main() {
	lis, err := net.Listen("tcp", ":9003")
	if err != nil {
		log.Fatalf("Failed to listen on port 9003: %v", err)
	}

	s3 := chat.Server{}
	estado := chat.Message {
		Body: "Libre",
	}
	s3.CambiarEstadoDis(context.Background(), &estado)
	grpcServer := grpc.NewServer()

	chat.RegisterChatServiceServer(grpcServer, &s3)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port 9003: %v", err)
	}
	fmt.Print("Escuchando")

}
