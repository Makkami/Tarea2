package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"log"
	"net"
	"math"
	"bufio"
	"strings"
	"math/rand"
	"time"

	"github.com/Makkami/Tarea2/chat"
	"google.golang.org/grpc"
)	


func crearServer() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := chat.Server{}
	grpcServer := grpc.NewServer()

	chat.RegisterChatServiceServer(grpcServer, &s)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("a %v", err)
	}
}

func ExisteLibro(arr []string, nombre string) bool {
	for _, numero := range arr {
		if numero == nombre {
			return true
		}
	}
	return false
}

func SubirLibro(op_algo string) {
	var reader = bufio.NewReader(os.Stdin)

	ports := []string{
		"dist137:9001",
		"dist138:9002",
		"dist139:9003",
	}

	// Conexion a NameNode para obtener titulos de libros subidos
	var connNN *grpc.ClientConn
	connNN, errNN := grpc.Dial(":9004", grpc.WithInsecure())
	if errNN != nil {
		fmt.Printf("No se pudo conectar al NameNode:  %s", errNN)
	}
	cNN := chat.NewChatServiceClient(connNN)

	defer connNN.Close()

	// Ver si ya esxiste el libro
	message := chat.Message{
		Body: "Biblioteca",
	}
	biblio, _ := cNN.PedirBiblioteca(context.Background(), &message)
	titulosLibros := strings.Split(biblio.Libros, "@")

	fmt.Printf("Ingrese nombre del libro que desea subir (sin el .pdf): ")
	libro, _ := reader.ReadString('\n')
	libro = strings.Trim(libro, " \r\n")
	if ExisteLibro(titulosLibros, libro) {
		fmt.Println("Ya fue subido un libro con ese nombre")
		return
	}
	fileToBeChunked := "./LibrosParaSubir/" + libro + ".pdf"
	
	flag1, flag2, flag3 := true, true, true
	/* Conexiones a los datanodes*/
		// Datanode 1
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(ports[0], grpc.WithInsecure())
	if err != nil {
		fmt.Printf("No se pudo conectar al DataNode 1:  %s", err)
	}
	c := chat.NewChatServiceClient(conn)

	defer conn.Close()

		// Datanode 2
	var conn2 *grpc.ClientConn
	conn2, err2 := grpc.Dial(ports[1], grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("No se pudo conectar al DataNode 2:  %s", err2)
	}
	c2 := chat.NewChatServiceClient(conn2)

	defer conn2.Close()

		// Datanode 3
	var conn3 *grpc.ClientConn
	conn3, err3 := grpc.Dial(ports[2], grpc.WithInsecure())
	if err3 != nil {
		fmt.Printf("No se pudo conectar al DataNode 3:  %s", err3)
	}
	c3 := chat.NewChatServiceClient(conn3)

	defer conn3.Close()

	//Estado DataNodes
	pregunta := chat.Message {
		Body: "Estado",
	}

	_, error1 := c.Estado(context.Background(), &pregunta)
	_, error2 := c2.Estado(context.Background(), &pregunta)
	_, error3 := c3.Estado(context.Background(), &pregunta)
	if error1 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 1")
		flag1 = false
	}
	if error2 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 2")
		flag2 = false
	}
	if error3 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 3")
		flag3 = false
	}

	//Elegir Datanode random
	sliceDN := []int{1,2,3}
	if (!flag1 && !flag2 && !flag3) {
		fmt.Println("No se pudo establecer conexión que ningun DataNode")
		os.Exit(1)
	} else if (!flag1 && !flag2) {sliceDN = []int{3}
	} else if !flag1 && !flag3 {sliceDN = []int{2}
	} else if !flag2 && !flag3 {sliceDN = []int{1}
	} else if !flag1 {sliceDN = []int{2,3}
	} else if !flag2 {sliceDN = []int{1,3}
	} else if !flag3 {sliceDN = []int{1,2}}
	fmt.Println("Lista de nodos activos: ", sliceDN)
	
	rand.Seed(time.Now().UnixNano())
	rdIndex := rand.Intn(len(sliceDN))
	dn_rand := sliceDN[rdIndex]
	fmt.Printf("Random: %d\n", dn_rand)
	
	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 250 * (1 << 10) // Este (1 << 10) es igual a 2^10, entonces es 250 * 1024 = 256000

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Dividiendo el archivo en %d partes.\n", totalPartsNum)
	var port string
	switch dn_rand {
	case 1:
		port = ports[0]
	case 2:
		port = ports[1]
	case 3:
		port = ports[2]
	}

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		message := chat.Chunk{
			Nombre: libro,
			Parte: strconv.FormatUint(i, 10),
			NumPartes: totalPartsNum,
			Buffer: partBuffer,
			Port: port,
			Algo: op_algo,
		}

		var response *chat.Message
		switch dn_rand {
		case 1:
			response, _ = c.ChunkClienteANodo(context.Background(), &message)
			log.Printf("DataNode1 %s", response.Body)
		case 2:
			response, _ = c2.ChunkClienteANodo(context.Background(), &message)
			log.Printf("DataNode2 %s", response.Body)
		case 3:
			response, _ = c3.ChunkClienteANodo(context.Background(), &message)
			log.Printf("DataNode3 %s", response.Body)
		}
	}
}

func DescargarLibro(){
	// Conexion con el NameNode
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist140:9004", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("No se pudo conectar al NameNode:  %s", err)
	}
	c := chat.NewChatServiceClient(conn)
	defer conn.Close()

	// Pedir y mostrar la lista de todos los libros disponibles (los que se encuentran en log.txt)
	message := chat.Message{
		Body: "Biblioteca",
	}
	biblio, _ := c.PedirBiblioteca(context.Background(), &message)
	titulosLibros := strings.Split(biblio.Libros, "@")
	if biblio.Largo == "0" {
		fmt.Println("No hay libros disponibles por el momento")
		return
	}
	var seleccionLibro string
	for {
		fmt.Println("-----------------------------------------")
		fmt.Println("Libros disponibles para descargar:")
		for i := 0; i < len(titulosLibros); i++ {
			fmt.Printf(strconv.Itoa(i+1) + ". " + titulosLibros[i] + "\n")
		}
		fmt.Println("-----------------------------------------")

		// Seleccionar el libro que se va a descargar
		fmt.Println("Selecciones numero del libro que desea descargar: ")
		fmt.Scanln(&seleccionLibro)
		selec, _ := strconv.Atoi(seleccionLibro)
		if selec <= len(titulosLibros) && selec > 0{
			break
		}else {
			fmt.Println("Eleccion incorrecta. Debe escoger uno de los libros del catalogo")
		}
	}	
	selec, _ := strconv.Atoi(seleccionLibro)
	
	nombre := chat.Message{
		Body: titulosLibros[selec-1],
	}
	
	// Buscar direcciones en donde se encuentra cada chunk del libro
	chunksLibro, _ := c.LogChunks(context.Background(), &nombre)
	listaPartes := strings.Split(chunksLibro.Libros, "@")

	// Establecer conexiones con los datanodes
	var conn1 *grpc.ClientConn
	conn1, err1 := grpc.Dial("dist137:9001", grpc.WithInsecure())
	if err1 != nil {
		fmt.Printf("No se pudo conectar al DataNode 1:  %s", err)
	}
	c1 := chat.NewChatServiceClient(conn1)
	defer conn1.Close()

	var conn2 *grpc.ClientConn
	conn2, err2 := grpc.Dial("dist138:9002", grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("No se pudo conectar al DataNode 2:  %s", err)
	}
	c2 := chat.NewChatServiceClient(conn2)
	defer conn2.Close()


	var conn3 *grpc.ClientConn
	conn3, err3 := grpc.Dial("dist139:9003", grpc.WithInsecure())
	if err3 != nil {
		fmt.Printf("No se pudo conectar al DataNode 3:  %s", err)
	}
	c3 := chat.NewChatServiceClient(conn3)
	defer conn3.Close()

	//Estado DataNodes
	flag1, flag2, flag3 := true, true, true
	pregunta := chat.Message {
		Body: "Estado",
	}

	_, error1 := c.Estado(context.Background(), &pregunta)
	_, error2 := c2.Estado(context.Background(), &pregunta)
	_, error3 := c3.Estado(context.Background(), &pregunta)
	if error1 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 1")
		flag1 = false
	}
	if error2 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 2")
		flag2 = false
	}
	if error3 != nil {
		fmt.Println("No se puedo establecer conexion con el DataNode 3")
		flag3 = false
	}

	if !flag1 || !flag2 || !flag3 {
		fmt.Println("Todos los DataNodes deben estar arriba para la descarga de libros")
		return
	}

	// Iterar por cada chunk del libro
	for i := 0; i < len(listaPartes); i++ {
		aux := strings.Split(listaPartes[i], " ")
		nombreparte := chat.Message{
			Body: aux[0],
		}
		if aux[1] == "dist137:9001" {
			res1, _ := c1.TraerChunk(context.Background(), &nombreparte)
			fmt.Println(res1)
		}else if aux[1] == "dist138:9002" {
			res2, _ := c2.TraerChunk(context.Background(), &nombreparte)
			fmt.Println(res2)
		}else if aux[1] == "dist139:9003" {
			res3, _ := c3.TraerChunk(context.Background(), &nombreparte)
			fmt.Println(res3)
		}
	}

	newFileName := "NEW" + nombre.Body + ".pdf"
	_, err = os.Create(newFileName)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	var writePosition int64 = 0

	for j := uint64(0); j < uint64(len(listaPartes)); j++ {

			currentChunkFileName := nombre.Body + "_" + strconv.FormatUint(j, 10)

			newFileChunk, err := os.Open(currentChunkFileName)

			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}

			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()

			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}

			var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)

			fmt.Println("Appending at position : [", writePosition, "] bytes")
			writePosition = writePosition + chunkSize

			// read into chunkBufferBytes
			reader := bufio.NewReader(newFileChunk)
			_, err = reader.Read(chunkBufferBytes)

			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}

			n, err := file.Write(chunkBufferBytes)

			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}

			file.Sync() //flush to disk

			chunkBufferBytes = nil 
			fmt.Println("Written ", n, " bytes")

			fmt.Println("Recombining part [", j, "] into : ", newFileName)
	}

	file.Close()
}

func main() {

	go crearServer()
	for {
		var op_cliente, op_algo string
		fmt.Println("----------------------")
		fmt.Println("Eliga una opcion: ")
		fmt.Println("1. Cliente uploader")
		fmt.Println("2. Cliente downloader")
		fmt.Println("----------------------")

		fmt.Scanln(&op_cliente)


		if strings.Compare(op_cliente, "1") == 0 {
			fmt.Println("----------------------")
			fmt.Println("Va a subir un libro, escoga el algoritmo que utilizara: ")
			fmt.Println("1. Centralizado")
			fmt.Println("2. Distribuido")
			fmt.Println("----------------------")

			fmt.Scanln(&op_algo)
			if strings.Compare(op_algo, "1") == 0{
				op_algo = "Centralizado"
				SubirLibro(op_algo)
			}else if strings.Compare(op_algo, "2") == 0 {
				op_algo = "Distribuido"
				SubirLibro(op_algo)
			}else {
				fmt.Println("Tiene que elegir una de las opciones mostradas")
			}
			
		} else if strings.Compare(op_cliente, "2") == 0 {
			fmt.Println("Descargar libro")
			DescargarLibro()
		} else {
			fmt.Println("Tiene que elegir una de las opciones mostradas")
		}
	}


}