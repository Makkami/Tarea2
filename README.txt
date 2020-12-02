--------------------------------------------
Jorge Juan 201666512-9
Maximiliano Ojeda 201773576-7
--------------------------------------------
Distribucion de los archivos:
  Cada maquina contiene todos los archivos necesarios para la ejecucion de la tarea junto con sus archivos "Makefile"
  Dist140: cliente.go, nodoname.go
  Dist137: nodo1.go
  Dist138: nodo2.go
  Dist139: nodo3.go
  
 Los archivos se encuentran en la carpeta "Tarea2"
--------------------------------------------
Los archivos deben ejecutarse en la carpeta "Tarea2" en el siguiente orden para su funcionamiento:
  1. Ejecutar el comando "make" en la maquina dist140 para iniciar el Name Node y el Cliente(Uploader y Downloader)
  2  Ejecutar el comando "make" en la maquina dist137 para iniciar el Data Node 1
  3. Ejecutar el comando "make" en la maquina dist138 para iniciar el Data Node 2
  4. Ejecutar el comando "make" en la maquina dist139 para iniciar el Data Node 3
--------------------------------------------
Notas:
-El nombre de los PDFs que se desean subir no pueden incluir un "_" en su nombre.
-Se asume que el Name Node no se apagara durante la ejecucion de la tarea.
-La eleccion de cliente uploader o downloader se muestra como opcion de un menu al iniciar el cliente.go
-La eleccion de los algoritmos para la distribucion de chunks se muestra como opcion de un menu al entrar como Cliente Uploader.
-Los libros que se van a subir se encuentran en la carpeta LibrosParaSubir.
-El nombre de los libros descargados sera "NEW" + "nombre_del_libro.pdf" para diferenciarlos de los origniales. Ej:NEWAlicia.pdf
-Debe entrar en la carpeta "Tarea2" para ejecutar los archivos.

El informe se llama SD_L2_Juan_Ojeda.pdf
