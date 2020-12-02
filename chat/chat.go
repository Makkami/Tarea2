package chat

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"bufio"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

//Server xd
type Server struct {
	dn1 []int
	dn2 []int
	dn3 []int
}

func conseguirChunk(Numero int, Name string) Chunk {
	var Envio Chunk
	return Envio
}

//transformarArregloS transforma una lista de ints a un string
func transformarArregloS(lista []int) string {
    Respuesta := ""
    for i := 0; i < len(lista); i++ {
        if i == 0 {
            Respuesta = strconv.Itoa(lista[i])
        }else{
            Respuesta = Respuesta + "@" + strconv.Itoa(lista[i])
        }
    }
    return Respuesta
}

//SubirChunk xd
func (s *Server) ChunkClienteANodo(ctx context.Context, message *Chunk) (*Message, error) {

	fileName := message.Nombre + "_" + message.Parte
	_, err := os.Create(fileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// write/save buffer to disk
	ioutil.WriteFile(fileName, message.Buffer, os.ModeAppend)


	
	fmt.Printf("Parte: %s; NumPartes: %s\n", message.Parte, strconv.FormatUint(message.NumPartes,10))

	if message.Parte == strconv.FormatUint(message.NumPartes-1,10) {
		if strings.Compare(message.Algo, "Centralizado") == 0 {
			fmt.Println("Entre a centralizado")
			res := propuestaDataNode(int(message.NumPartes))
			res2 := PropuestaRespuesta {
				Nombre: message.Nombre,
				Total: strconv.FormatUint(message.NumPartes,10),
				Nd1: res.Nd1,
				Nd2: res.Nd2,
				Nd3: res.Nd3,
				Intn1: res.Intn1,
				Intn2: res.Intn2,
				Intn3: res.Intn3,
			}
			/*
			fmt.Printf("Lista nd1: %s\n", res2.Nd1)
			fmt.Printf("Lista nd2: %s\n", res2.Nd2)
			fmt.Printf("Lista nd3: %s\n", res2.Nd3)
			fmt.Printf("Largo lista nd1: %s\n", res2.Intn1)
			fmt.Printf("Largo lista nd2: %s\n", res2.Intn2)
			fmt.Printf("Largo lista nd3: %s\n", res2.Intn3)
			*/
			var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist140:9004", grpc.WithInsecure())
			if err != nil {
				fmt.Printf("No se pudo conectar con el NameNode:  %s", err)
			}
			c := NewChatServiceClient(conn)
			defer conn.Close()

			prop, _ := c.AceptarPropuesta(context.Background(), &res2)
			response, _ := RepartirChunks(prop)
			fmt.Println(response)

		}else if strings.Compare(message.Algo, "Distribuido") == 0 {
			AlgoritmoDistribuido()
		}
	}

	return &Message{Body: "Se envio correctamente el chunk"}, nil
}


func propuestaDataNode(total int) Propuesta {
	var lista1 []int
	var lista2 []int
	var lista3 []int
	i := 0
	for {
		if i < total {
			lista1 = append(lista1, i) //[0][3][][]
			i++
		} else {
			break
		}
		//fmt.Println(i)
		if i < total {
			lista2 = append(lista2, i) //[1][4][][]
			i++
		} else {
			break
		}
		//fmt.Println(i)
		if i < total {
			lista3 = append(lista3, i) //[2][][][]
			i++
		} else {
			break
		}
		//fmt.Println(i)

	}
	largo1 := strconv.Itoa(len(lista1))
	largo2 := strconv.Itoa(len(lista2))
	largo3 := strconv.Itoa(len(lista3))
	st1 := transformarArregloS(lista1)
	st2 := transformarArregloS(lista2)
	st3 := transformarArregloS(lista3)
	respuesta := Propuesta{
		Nd1:   st1,
		Nd2:   st2,
		Nd3:   st3,
		Intn1: largo1,
		Intn2: largo2,
		Intn3: largo3,
	}
	return respuesta
}

func propuesta(total int) Propuesta {
	flag1 := true
	flag2 := true
	flag3 := true

	var conn1 *grpc.ClientConn
	conn1, err1 := grpc.Dial("dist137:9001", grpc.WithInsecure())
	if err1 != nil {
		fmt.Printf("No se pudo conectar al DataNode 1:  %s", err1)
	}
	c1 := NewChatServiceClient(conn1)

	defer conn1.Close()

		// Datanode 2
	var conn2 *grpc.ClientConn
	conn2, err2 := grpc.Dial("dist138:9002", grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("No se pudo conectar al DataNode 2:  %s", err2)
	}
	c2 := NewChatServiceClient(conn2)

	defer conn2.Close()

		// Datanode 3
	var conn3 *grpc.ClientConn
	conn3, err3 := grpc.Dial("dist139:9003", grpc.WithInsecure())
	if err3 != nil {
		fmt.Printf("No se pudo conectar al DataNode 3:  %s", err3)
	}
	c3 := NewChatServiceClient(conn3)

	defer conn3.Close()

	//Estado DataNodes
	pregunta := Message {
		Body: "Estado",
	}

	_, error1 := c1.Estado(context.Background(), &pregunta)
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
	var lista1 []int
	var lista2 []int
	var lista3 []int
	i := 0
	for {
		if flag1 == true {
			if i < total {
				lista1 = append(lista1, i) //[0][3][][]
				i++
			} else {
				break
			}
		}
		if flag2 == true {
			if i < total {
				lista2 = append(lista2, i) //[1][4][][]
				i++
			} else {
				break
			}
		}
		if flag3 == true {
			if i < total {
				lista3 = append(lista3, i) //[2][][][]
				i++
			} else {
				break
			}
		}
	}
	largo1 := strconv.Itoa(len(lista1))
	largo2 := strconv.Itoa(len(lista2))
	largo3 := strconv.Itoa(len(lista3))
	st1 := transformarArregloS(lista1)
	st2 := transformarArregloS(lista2)
	st3 := transformarArregloS(lista3)
	respuesta := Propuesta{
		Nd1:   st1,
		Nd2:   st2,
		Nd3:   st3,
		Intn1: largo1,
		Intn2: largo2,
		Intn3: largo3,
	}
	return respuesta
}


//escribirLog escribe el archivo
func escribirLog(log PropuestaRespuesta) {
	Titulo := log.Nombre
	total := log.Total
	file, err := os.OpenFile("log.txt", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		fmt.Println(err)
		return
	}
	largo1, err := strconv.Atoi(log.Intn1)
	largo2, err := strconv.Atoi(log.Intn2)
	largo3, err := strconv.Atoi(log.Intn3)
	listan1 := strings.SplitN(log.Nd1, "@", largo1)
	listan2 := strings.SplitN(log.Nd2, "@", largo2)
	listan3 := strings.SplitN(log.Nd3, "@", largo3)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	fmt.Fprintf(file, "%s %s\n", Titulo, total)

	for i := 0; i < largo1; i++ {
		fmt.Fprintf(file, "%s_%s %s\n", Titulo, listan1[i], "dist137:9001")
	}

	for i := 0; i < largo2; i++ {
		fmt.Fprintf(file, "%s_%s %s\n", Titulo, listan2[i], "dist138:9002")
	}

	for i := 0; i < largo3; i++ {
		fmt.Fprintf(file, "%s_%s %s\n", Titulo, listan3[i], "dist139:9003")
	}

	fmt.Fprintf(file, "\n")
}


func (s *Server) AceptarPropuesta(ctx context.Context, message *PropuestaRespuesta) (*PropuestaRespuesta, error) {
	flag1 := true
	flag2 := true
	flag3 := true

	var conn1 *grpc.ClientConn
	conn1, err1 := grpc.Dial("dist137:9001", grpc.WithInsecure())
	if err1 != nil {
		fmt.Printf("No se pudo conectar al DataNode 1:  %s", err1)
	}
	c1 := NewChatServiceClient(conn1)

	defer conn1.Close()

		// Datanode 2
	var conn2 *grpc.ClientConn
	conn2, err2 := grpc.Dial("dist138:9002", grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("No se pudo conectar al DataNode 2:  %s", err2)
	}
	c2 := NewChatServiceClient(conn2)

	defer conn2.Close()

		// Datanode 3
	var conn3 *grpc.ClientConn
	conn3, err3 := grpc.Dial("dist139:9003", grpc.WithInsecure())
	if err3 != nil {
		fmt.Printf("No se pudo conectar al DataNode 3:  %s", err3)
	}
	c3 := NewChatServiceClient(conn3)

	defer conn3.Close()

	//Estado DataNodes
	pregunta := Message {
		Body: "Estado",
	}

	_, error1 := c1.Estado(context.Background(), &pregunta)
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

	var respuesta PropuestaRespuesta

	if flag1 == false && message.Intn1 == "0" {
		flag1 = true
	} else if flag1 == true {
		flag1 = true
	}else{
		flag1 = false
	}

	if flag2 == false && message.Intn2 == "0" {
		flag2 = true
	} else if flag2 == true {
		flag2 = true
	} else {
		flag2 = false
	}

	if flag3 == false && message.Intn3 == "0" {
		flag3 = true
	} else if flag3 == true {
		flag3 = true
	} else {
		flag3 = false
	}
	if flag1 == true && flag2 == true && flag3 == true {
		respuesta = *message
	} else {
		fmt.Println("Entra a propuesta")
		intero, err := strconv.Atoi(message.Total)
		if err != nil {
			log.Fatalf("Error en transformar strings en int: %s", err)
		}
		aux := propuesta(intero)
		respuesta = PropuestaRespuesta{
			Nombre: message.Nombre,
			Total:  message.Total,
			Nd1:    aux.Nd1,
			Nd2:    aux.Nd2,
			Nd3:    aux.Nd3,
			Intn1:  aux.Intn1,
			Intn2:  aux.Intn2,
			Intn3:  aux.Intn3,
		}
	}
	fmt.Println("Partes para cada DataNode: ")
	fmt.Println(respuesta.Nd1)
	fmt.Println(respuesta.Nd2)
	fmt.Println(respuesta.Nd3)
	escribirLog(respuesta)
	return &respuesta, nil
}

func RepartirChunks(message *PropuestaRespuesta) (*Message, error){
	largo1, err := strconv.Atoi(message.Intn1)
	largo2, err := strconv.Atoi(message.Intn2)
	largo3, err := strconv.Atoi(message.Intn3)
	listan1 := strings.SplitN(message.Nd1, "@", largo1)
	listan2 := strings.SplitN(message.Nd2, "@", largo2)
	listan3 := strings.SplitN(message.Nd3, "@", largo3)
	if err != nil {
		fmt.Println(err)
	}
	if largo1 != 0 {
		var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist137:9001", grpc.WithInsecure())
			if err != nil {
				fmt.Printf("No se pudo conectar con el DataNode 1:  %s", err)
			}
			c1 := NewChatServiceClient(conn)
			defer conn.Close()

		for i := 0; i < largo1; i++ {
			title := message.Nombre
			part := listan1[i]
			newFileChunk, err := os.Open(title + "_" + part)			

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()            // calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant


            var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)
			
			chunkToSend := Chunk {
				Nombre: title,
				Parte: part,
				NumPartes: uint64(largo1+largo2+largo3),
				Buffer: chunkBufferBytes,
				Port: "dist137:9001",
				Algo: "",
			}
			
			respuesta, _ := c1.EnviarChunksEntreNodos(context.Background(), &chunkToSend)
			fmt.Println(respuesta)
		}
	}
	if largo2 != 0 {
		var conn2 *grpc.ClientConn
			conn2, err2 := grpc.Dial("dist138:9002", grpc.WithInsecure())
			if err2 != nil {
				fmt.Printf("No se pudo conectar con el DataNode 2:  %s", err)
			}
			c2 := NewChatServiceClient(conn2)
			defer conn2.Close()

		for i := 0; i < largo2; i++ {
			title := message.Nombre
			part := listan2[i]
			newFileChunk, err := os.Open(title + "_" + part)			

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()            // calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant


            var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)
			
			chunkToSend := Chunk {
				Nombre: title,
				Parte: part,
				NumPartes: uint64(largo1+largo2+largo3),
				Buffer: chunkBufferBytes,
				Port: "dist138:9002",
				Algo: "",
			}
			
			respuesta2, _ := c2.EnviarChunksEntreNodos(context.Background(), &chunkToSend)
			fmt.Println(respuesta2)


		}
	}
	if largo3 != 0 {
		var conn3 *grpc.ClientConn
			conn3, err3 := grpc.Dial("dist139:9003", grpc.WithInsecure())
			if err3 != nil {
				fmt.Printf("No se pudo conectar con el DataNode 2:  %s", err)
			}
			c3 := NewChatServiceClient(conn3)
			defer conn3.Close()

		for i := 0; i < largo3; i++ {
			title := message.Nombre
			part := listan3[i]
			newFileChunk, err := os.Open(title + "_" + part)			

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()            // calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant


            var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)
			
			chunkToSend := Chunk {
				Nombre: title,
				Parte: part,
				NumPartes: uint64(largo1+largo2+largo3),
				Buffer: chunkBufferBytes,
				Port: "dist139:9003",
				Algo: "",
			}
			
			respuesta3, _ := c3.EnviarChunksEntreNodos(context.Background(), &chunkToSend)
			fmt.Println(respuesta3)
		}
	}
	return &Message{Body: "Chunks repartidos exitosamente"}, nil
}

func (s *Server) EnviarChunksEntreNodos(ctx context.Context, message *Chunk) (*Message, error){
	fileName := message.Nombre + "_" + message.Parte
	_, err := os.Create(fileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// write/save buffer to disk
	ioutil.WriteFile(fileName, message.Buffer, os.ModeAppend)
	fmt.Println("Recibido chunk " + message.Parte + " de " + message.Nombre)

	return &Message{Body: "Chunk " + message.Parte + " repartido en la ip " + message.Port}, nil
}

//pedirBiblioteca te envia los libros que hay en el directorio
func (s *Server) PedirBiblioteca(ctx context.Context, message *Message) (*Biblioteca, error) {
    f, err := os.Open("log.txt")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

    scanner := bufio.NewScanner(f)
    var lista []string
    var titulo string
    var nombre string
    var largo int

    for scanner.Scan() {
        if err := scanner.Err(); err != nil {
            log.Fatal(err)
        }
        titulo = scanner.Text()
        aux := strings.SplitN(titulo, " ", 2)
        if len(aux) != 1 {
            largo, err = strconv.Atoi(aux[1])
            nombre = aux[0]
            lista = append(lista, nombre)
        } else {
            largo = 0
        }
        for i := 0; i < largo; i++ {
            scanner.Scan()
        }
    }
    respuesta := Biblioteca{
        Largo:  strconv.Itoa(len(lista)),
        Libros: transformarListaS(lista),
    }
    return &respuesta, nil
}

func transformarListaS(lista []string) string {
    Respuesta := ""
    for i := 0; i < len(lista); i++ {
        if i == 0 {
            Respuesta = lista[i]
        } else {
            Respuesta = Respuesta + "@" + lista[i]
        }
    }
    return Respuesta
}

func (s *Server) LogChunks(ctx context.Context, message *Message) (*Biblioteca, error) {
    f, err := os.Open("log.txt")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

    scanner := bufio.NewScanner(f)
    var lista []string
    var titulo string
    var nombre string
    var largo int

    for scanner.Scan() {
        if err := scanner.Err(); err != nil {
            log.Fatal(err)
        }
		titulo = scanner.Text()
        aux := strings.SplitN(titulo, " ", 2)
        if len(aux) != 1 {
            largo, err = strconv.Atoi(aux[1])
            nombre = aux[0]
            if nombre == message.Body {
                for i := 0; i < largo; i++ {
                    scanner.Scan()
                    lista = append(lista, scanner.Text())
                }
                break
            }

        } else {
            largo = 0
        }
        for i := 0; i < largo; i++ {
            scanner.Scan()
        }
	}
    respuesta := Biblioteca{
        Largo:  strconv.Itoa(len(lista)),
        Libros: transformarListaS(lista),
	}
    return &respuesta, nil
}

func (s *Server) TraerChunk(ctx context.Context, message *Message) (*Message, error) {
	aux := strings.Split(message.Body, "_")
	nombre := aux[0]
	parte := aux[1]

	// Conexion con el cliente
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist140:9000", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("No se pudo conectar con el Cliente Downloader:  %s", err)
	}
	c := NewChatServiceClient(conn)
	defer conn.Close()

	newFileChunk, err := os.Open(nombre + "_" + parte)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer newFileChunk.Close()

	chunkInfo, err := newFileChunk.Stat()          

	var chunkSize int64 = chunkInfo.Size()
	chunkBufferBytes := make([]byte, chunkSize)
	
	chunkToSend := Chunk {
		Nombre: nombre,
		Parte: parte,
		NumPartes: 1,
		Buffer: chunkBufferBytes,
		Port: ":9000",
		Algo: "",
	}

	respuesta, _ := c.EnviarChunksEntreNodos(context.Background(), &chunkToSend)
	fmt.Println(respuesta)
	return &Message{Body: "Chunk "+ parte +" enviado al cliente"}, nil

}

func (s *Server) Estado(ctx context.Context, message *Message) (*Message, error){
	return &Message{Body: "Up"}, nil
}

func AlgoritmoDistribuido() {

}