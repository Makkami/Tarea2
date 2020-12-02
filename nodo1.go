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
La logica.go solo es el servidor levandado. las funciones con relacion a la logica estan en chat.go
*/
func main() {
	// Creacion de servidor que escucha en el puesto 9001
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("Failed to listen on port 9001: %v", err)
	}
	// Se establece el estado del nodo como "Libre"
	s1 := chat.Server{}
	estado := chat.Message {
		Body: "Libre",
	}
	s1.CambiarEstadoDis(context.Background(), &estado)
	grpcServer := grpc.NewServer()

	chat.RegisterChatServiceServer(grpcServer, &s1)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port 9001: %v", err)
	}
	fmt.Print("Escuchando")

}
