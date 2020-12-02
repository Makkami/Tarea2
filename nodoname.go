package main

import (
	"log"
	"net"
	"os"
	"fmt"

	"github.com/Makkami/Tarea2/chat"
	"google.golang.org/grpc"
)

/*Funcion main
La logica.go solo es el servidor levandado. las funciones con relacion a la logica estan en chat.go
*/
func main() {

	// Busca si existe un archivo log,txt
	f, err1 := os.Open("log.txt")
    if err1 == nil {
        fmt.Println("Archivo log.txt existente")
    }
	f.Close()
	
	// Si no hay un log.txt, se crea uno
	if err1 != nil {
		_, err := os.Create("log.txt")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Archivo log.txt creado")
	}

	// Se crear servidor que escucha en el puerto :9004
	lis, err := net.Listen("tcp", ":9004")
	if err != nil {
		log.Fatalf("Failed to listen on port 9004: %v", err)
	}

	sN := chat.Server{}

	grpcServer := grpc.NewServer()

	chat.RegisterChatServiceServer(grpcServer, &sN)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port 9004: %v", err)
	}
}
