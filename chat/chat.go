package chat

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"bufio"
	"math/rand"
	"time"
	"sync"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)


type Server struct {
	dn1 []int
	dn2 []int
	dn3 []int
	estado string
	mux sync.Mutex
}

//transformarArregloS transforma una lista de ints a un string con separacion "@"
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

// ChunkClienteANodo realiza la distribucion de los chunks a las distintas maquina utilizando
// las otras funciones. Se realiza la distribucion dependiendo si se es centralizada o distribuida
func (s *Server) ChunkClienteANodo(ctx context.Context, message *Chunk) (*Message, error) {

	fileName := message.Nombre + "_" + message.Parte
	_, err := os.Create(fileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// write/save buffer to disk
	ioutil.WriteFile(fileName, message.Buffer, os.ModeAppend)
	if message.Parte == strconv.FormatUint(message.NumPartes-1,10) {
		// Centralizado
		if strings.Compare(message.Algo, "Centralizado") == 0 {
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
			// Se crea conexion al NameNode
			var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist140:9004", grpc.WithInsecure())
			if err != nil {
				fmt.Printf("No se pudo conectar con el NameNode:  %s", err)
			}
			c := NewChatServiceClient(conn)
			defer conn.Close()

			// Se solicita la aceptacion de la propuesta o la generacion de una nueva
			prop, _ := c.AceptarPropuesta(context.Background(), &res2)
			response, _ := RepartirChunks(prop)
			fmt.Println(response)
		
		// Distribuido
		}else if strings.Compare(message.Algo, "Distribuido") == 0 {
			ports := []string{"dist137:9001","dist138:9002","dist139:9003"}
			var distintos []string
			for i := 0; i < len(ports); i++ {
				if ports[i] != message.Port {
					distintos = append(distintos, ports[i])
				}
			}

			// Conexion a los DataNodes distintos
			flag1, flag2 := true, true
			var conn1 *grpc.ClientConn

			conn1, err1 := grpc.Dial(distintos[0], grpc.WithInsecure())
			if err1 != nil {
				fmt.Printf("No se pudo conectar con el DataNode:  %s", err1)
			}
			c1 := NewChatServiceClient(conn1)
			defer conn1.Close()

			var conn2 *grpc.ClientConn
			conn2, err2 := grpc.Dial(distintos[1], grpc.WithInsecure())
			if err2 != nil {
				fmt.Printf("No se pudo conectar con el DataNode:  %s", err2)
			}
			c2 := NewChatServiceClient(conn2)
			defer conn2.Close()

			// Estado de los DataNodes
			pregunta := Message {
				Body: "Estado",
			}
		
			_, error1 := c1.Estado(context.Background(), &pregunta)
			_, error2 := c2.Estado(context.Background(), &pregunta)

			if error1 != nil {
				fmt.Println("No se puedo establecer conexion con el DataNode 1")
				flag1 = false
			}
			if error2 != nil {
				fmt.Println("No se puedo establecer conexion con el DataNode 2")
				flag2 = false
			}

			// Crea conexion al nodoactual
			var connA *grpc.ClientConn
			connA, errA := grpc.Dial(distintos[1], grpc.WithInsecure())
			if errA != nil {
				fmt.Printf("No se pudo conectar con el DataNode:  %s", errA)
			}
			cA := NewChatServiceClient(connA)
			defer connA.Close()

			// Crea conexion al NameNode
			var connNN *grpc.ClientConn
			connNN, errNN := grpc.Dial("dist140:9004", grpc.WithInsecure())
			if errNN != nil {
				fmt.Printf("No se pudo conectar con el NameNode:  %s", errNN)
			}
			cNN := NewChatServiceClient(connNN)
			defer connNN.Close()

			// Propuesta de distribucion equitativa en los 3 datanodes
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
			dummy := Message {
				Body: "Dummy",
			}

			// Cantidad de chunks
			intero, err := strconv.Atoi(res2.Total)
			if err != nil {
				log.Fatalf("Error en transformar strings en int: %s", err)
			}

			// Caso en que los otros dos datanodes esten arriba
			if flag1 && flag2{
				// Se acepta o se rechaza la propuesta
				respuesta, _ := c1.EnviarPropuestaDN(context.Background(), &res2)
				respuesta2, _ := c2.EnviarPropuestaDN(context.Background(), &res2)
				if respuesta.Body == "Aceptada" && respuesta2.Body == "Aceptada" {
					for {
						// Se verifica si los datanodes estan "Libres" u "Ocupados"
						estado1, _ := c1.VerEstadoDis(context.Background(), &dummy)
						estado2, _ := c2.VerEstadoDis(context.Background(), &dummy)
						if estado1.Body == "Libre" && estado2.Body == "Libre" {
							// Se reaparten los chunks
							response, _ := RepartirChunks(&res2)
							fmt.Println(response)
							// Se cambia el estado del DataNode
							new_estado := Message {
								Body: "Ocupado",
							}
							cA.CambiarEstadoDis(context.Background(), &new_estado)
							s.mux.Lock()
							// Se escribe en el log.txt
							cNN.EscribirLog(context.Background(), &res2)
							s.mux.Unlock()
							// Se restaura el estado del DataNode
							next_estado := Message {
								Body: "Libre",
							}
							cA.CambiarEstadoDis(context.Background(),&next_estado)
							break
						}else{
							// Se esperan 3 segundos antes de intentar nuevamente
							time.Sleep(time.Second * 3)
						}
					} 
				}
			// Caso en que solo el primer datanode este arriba
			}else if flag1 && !flag2 {
				// Se crea nueva propuesta
				nueva := propuesta(intero)
				res2 = PropuestaRespuesta {
					Nombre: message.Nombre,
					Total: strconv.FormatUint(message.NumPartes,10),
					Nd1: nueva.Nd1,
					Nd2: nueva.Nd2,
					Nd3: nueva.Nd3,
					Intn1: nueva.Intn1,
					Intn2: nueva.Intn2,
					Intn3: nueva.Intn3,
				}
				// Se acepta o se rechaza la propuesta
				respuesta, _ := c1.EnviarPropuestaDN(context.Background(), &res2)
				if respuesta.Body == "Aceptada" {
					for {
						// Se verifica si los datanodes estan "Libres" u "Ocupados"
						estado1, _ := c1.VerEstadoDis(context.Background(), &dummy)
						if estado1.Body == "Libre" {
							// Se reaparten los chunks
							response, _ := RepartirChunks(&res2)
							fmt.Println(response)
							// Se cambia el estado del DataNode
							new_estado := Message {
								Body: "Ocupado",
							}
							cA.CambiarEstadoDis(context.Background(), &new_estado)
							s.mux.Lock()
							// Se escribe en el log.txt
							cNN.EscribirLog(context.Background(), &res2)
							s.mux.Unlock()
							next_estado := Message {
								Body: "Libre",
							}
							// Se restaura el estado del DataNode
							cA.CambiarEstadoDis(context.Background(),&next_estado)
							break
						}else{
							// Se esperan 3 segundos antes de intentar nuevamente
							time.Sleep(time.Second * 3)
						
						}
					}
				}
			// Caso en que solo el segundo datanode este arriba	
			}else if flag2 && !flag1 {
				// Se crea nueva propuesta
				nueva := propuesta(intero)
				res2 = PropuestaRespuesta {
					Nombre: message.Nombre,
					Total: strconv.FormatUint(message.NumPartes,10),
					Nd1: nueva.Nd1,
					Nd2: nueva.Nd2,
					Nd3: nueva.Nd3,
					Intn1: nueva.Intn1,
					Intn2: nueva.Intn2,
					Intn3: nueva.Intn3,
				}
				// Se acepta o se rechaza la propuesta
				respuesta2, _ := c2.EnviarPropuestaDN(context.Background(), &res2)
				if respuesta2.Body == "Aceptada" {
					for {
						// Se verifica si los datanodes estan "Libres" u "Ocupados"
						estado2, _ := c2.VerEstadoDis(context.Background(), &dummy)
						if estado2.Body == "Libre" {
							// Se reaparten los chunks
							response, _ := RepartirChunks(&res2)
							fmt.Println(response)
							// Se cambia el estado del DataNode
							new_estado := Message {
								Body: "Ocupado",
							}
							cA.CambiarEstadoDis(context.Background(), &new_estado)
							s.mux.Lock()
							// Se escribe en el log.txt
							cNN.EscribirLog(context.Background(), &res2)
							s.mux.Unlock()
							next_estado := Message {
								Body: "Libre",
							}
							// Se restaura el estado del DataNode
							cA.CambiarEstadoDis(context.Background(),&next_estado)
							break
						}else{
							// Se esperan 3 segundos antes de intentar nuevamente
							time.Sleep(time.Second * 3)
						
						}
					}
				}
			}
		}
	}

	return &Message{Body: "Se envio correctamente el chunk"}, nil
}

// propuestaDataNode recibe la cantidad de chunks de un libro y propone
// separarlos de manea equitativa en cada datanode 
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
	// Cantidad de chunks para cada datanode
	largo1 := strconv.Itoa(len(lista1))
	largo2 := strconv.Itoa(len(lista2))
	largo3 := strconv.Itoa(len(lista3))
	// Junta las listas con separacion "@"
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

// propuesta genera una nueva propuesta en caso de que la anterior haya sido rechazada
// retorna una nueva propuesta factible
func propuesta(total int) Propuesta {
	flag1 := true
	flag2 := true
	flag3 := true
		// Datanode 1
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
	// Cantidad de chunks para cada datanode
	largo1 := strconv.Itoa(len(lista1))
	largo2 := strconv.Itoa(len(lista2))
	largo3 := strconv.Itoa(len(lista3))
	// Junta las listas con separacion "@"
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
// EscribirLog para Algoritmo Distribuido escribe en el archivo log los libros con las direcciones en la cual
// se encuentra cada parte retornando un mensaje
func (s *Server) EscribirLog(ctx context.Context, log *PropuestaRespuesta) (*Message, error) {
	Titulo := log.Nombre
	total := log.Total
	// Creacion de log.txt o apertura
	file, err := os.OpenFile("log.txt", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		fmt.Println(err)
		return &Message{Body: "Error"}, nil
	}
	largo1, err := strconv.Atoi(log.Intn1)
	largo2, err := strconv.Atoi(log.Intn2)
	largo3, err := strconv.Atoi(log.Intn3)
	listan1 := strings.SplitN(log.Nd1, "@", largo1)
	listan2 := strings.SplitN(log.Nd2, "@", largo2)
	listan3 := strings.SplitN(log.Nd3, "@", largo3)
	if err != nil {
		fmt.Println(err)
		return &Message{Body: "Error"}, nil
	}
	defer file.Close()
	// Escritura en el log.txt
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
	return &Message{Body: "Chunk agregados al log.txt"}, nil
}

// escribirLog escribe en el archivo log los libros con las direcciones en la cual
// se encuentra cada parte
func escribirLog(log PropuestaRespuesta) {
	Titulo := log.Nombre
	total := log.Total
	// Creacion de log.txt o apertura
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
	// Escritura en el log.txt
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

// EnviarPropuestaDN puede aceptar o rechazar la propuesta recibida mediante porcentajes
// se escoge un numero entre el 1 al 100, si es mayor que 10 se acepta, sino se rechaza
func (s *Server) EnviarPropuestaDN(ctx context.Context, prop *PropuestaRespuesta) (*Message, error){
	rand.Seed(time.Now().UnixNano())
	rdnum := rand.Intn(100 - 0 + 1) + 1
	if rdnum > 10 {
		return &Message{Body: "Aceptada"}, nil
	}else{
		return &Message{Body: "Rechazada"}, nil
	}
		
}

// AceptarPropuesta verifica si la propuesta recibida es factible al ver el estado de los DataNodes
// si es factible, acepta la propuesta. Si se rechaza, se genera una nueva con los DataNodes disponibles
func (s *Server) AceptarPropuesta(ctx context.Context, message *PropuestaRespuesta) (*PropuestaRespuesta, error) {
	flag1 := true
	flag2 := true
	flag3 := true
	s.mux.Lock()
		// Datanode 1
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

	// Se revisan si uno de los nodos caidos esta siendo considerado en la reparticion de chunks (largo de lista > 0)
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
		// Se genera una nueva propuesta
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
	s.mux.Unlock()
	return &respuesta, nil
}

// RepartirChunks recibe la propuesta definitiva de distribucion de los chunks en las maquina
// y los crea en dichas direcciones
func RepartirChunks(message *PropuestaRespuesta) (*Message, error){
	// Cantidad de chunks para cada maquina
	largo1, err := strconv.Atoi(message.Intn1)
	largo2, err := strconv.Atoi(message.Intn2)
	largo3, err := strconv.Atoi(message.Intn3)
	// Listas de las partes de los chunks
	listan1 := strings.SplitN(message.Nd1, "@", largo1)
	listan2 := strings.SplitN(message.Nd2, "@", largo2)
	listan3 := strings.SplitN(message.Nd3, "@", largo3)
	if err != nil {
		fmt.Println(err)
	}
	if largo1 != 0 {
		// Se establece conexion con el DataNode 1
		var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist137:9001", grpc.WithInsecure())
			if err != nil {
				fmt.Printf("No se pudo conectar con el DataNode 1:  %s", err)
			}
			c1 := NewChatServiceClient(conn)
			defer conn.Close()
		// Se itera por la cantidad de chunks que van en el DataNode 1
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
			// Contenido del chunk
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
		// Se establece conexion con el DataNode 2
		var conn2 *grpc.ClientConn
			conn2, err2 := grpc.Dial("dist138:9002", grpc.WithInsecure())
			if err2 != nil {
				fmt.Printf("No se pudo conectar con el DataNode 2:  %s", err)
			}
			c2 := NewChatServiceClient(conn2)
			defer conn2.Close()
		// Se itera por la cantidad de chunks que van en el DataNode 2
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
			// Contenido del chunk			
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
		// Se establece conexion con el DataNode 3
		var conn3 *grpc.ClientConn
			conn3, err3 := grpc.Dial("dist139:9003", grpc.WithInsecure())
			if err3 != nil {
				fmt.Printf("No se pudo conectar con el DataNode 2:  %s", err)
			}
			c3 := NewChatServiceClient(conn3)
			defer conn3.Close()
		// Se itera por la cantidad de chunks que van en el DataNode 3
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
			// Contenido del chunk			
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
// EnviarChunksEntreNodos envia un chunk a otro nodo creando un archivo binario que tiene
// como nombre el titulo del libro mas la parte
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

//PedirBiblioteca te envia los libros que hay en el directorio como un string con separacion "@"
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
// transformarListaS junta los nombres de las partes de los chunks con "@"
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
// LogChunks busca el libro que se va a descargar y entrega un string con las direcciones
// de donde se encuentran los chunks de dicho libro
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
// TraerChunk toma los chunks de un datanode y los envia al cliente
// recibe un string del nombre y la parte del chunk y lo envia mediante Protocol Buffer
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

	// Abrir archivo con el nombre del chunk
	newFileChunk, err := os.Open(nombre + "_" + parte)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer newFileChunk.Close()

	chunkInfo, err := newFileChunk.Stat()          

	var chunkSize int64 = chunkInfo.Size()
	chunkBufferBytes := make([]byte, chunkSize)
	
	// Contenido del chunk a enviar al cliente
	chunkToSend := Chunk {
		Nombre: nombre,
		Parte: parte,
		NumPartes: 1,
		Buffer: chunkBufferBytes,
		Port: "dist140:9000",
		Algo: "",
	}
	// Enviar el chunk mediante la funcion EnviarChunksEntreNodos
	respuesta, _ := c.EnviarChunksEntreNodos(context.Background(), &chunkToSend)
	fmt.Println(respuesta)
	return &Message{Body: "Chunk "+ parte +" enviado al cliente"}, nil

}

// Estado avisa si un datanode esta "caido" mediante el envio de un mensaje
// recibe un mensaje y retorno otro verificando la conexion
func (s *Server) Estado(ctx context.Context, message *Message) (*Message, error){
	return &Message{Body: "Up"}, nil
}

// CambiarEstadoDis cambia el estado de un datanode de "Libre" a "Ocupado" o viceversa
// recibe un string del nuevo estado del datanode y retorna un mensaje avisando que se cambio
func (s *Server) CambiarEstadoDis(ctx context.Context, message *Message) (*Message, error) {
	s.estado = message.Body
	return &Message{Body: "Cambio de estado a " + message.Body}, nil
}

// VerEstadoDis ve el estado que tiene un datanode, si esta "Ocupado" o "Libre".
// recibe un mensaje y retorna el estado del datanode
func (s *Server) VerEstadoDis(ctx context.Context, message *Message) (*Message, error) {
	estado_actual := s.estado
	return &Message{Body: estado_actual}, nil
}

