package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func (s *store) Append(Content []byte) (bytes uint64, off uint64, err error) {
	//Lock y Unlock para resguardar los datos
	s.mu.Lock()
	defer s.mu.Unlock()

	// Hacemos el flush
	if err := s.buf.Flush(); err != nil {
		return 0, 0, err
	}

	// obtenemos la longitud para agregar y escribir
	off = s.size

	// ENcontramos la posicion para escribir
	if err := binary.Write(s.buf, enc, uint64(len(Content))); err != nil {
		return 0, 0, err
	}
	// Ahora si escribimos
	if err := binary.Write(s.buf, enc, Content); err != nil {
		return 0, 0, err
	}

	// Actualizamos el valore de la longitud, pues ya hemos agregado el contenido
	s.size = s.size + (uint64(lenWidth) + uint64(len(Content)))

	// regresamos el valor
	return s.size, off, err
}

func (s *store) Read(offset_in uint64) (offset_out []byte, err error) {
	//Leer el archivo desde un offset

	//Primero hacemos el flush
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	//tamaño en bytes para llegar al store
	size := make([]byte, lenWidth)

	//llamamos a la funcion ReadAt para leer
	if _, err := s.File.ReadAt(size, int64(offset_in)); err != nil {
		return nil, err // SI hubo un error, devolverlo
	}

	//encodeamos el valor antes sacado para llamar nuevamente la funcion REadAt
	size_encoded := enc.Uint64(size)
	content := make([]byte, size_encoded)

	if _, err := s.File.ReadAt(content, int64(offset_in+lenWidth)); err != nil {
		return nil, err //SI hay error, devolverlo
	}

	//devolver el nuevo valor del offset y el error nulo
	return content, err

}

func newStore(f *os.File) (*store, error) {
	//Hacer un nuevo store

	stats, err := f.Stat()

	if err != nil { //En caso de haber un error, devolverlo
		return nil, err
	}
	// SI no hay error, entonces devolver el nuevo store con la estructura que vimos al inicio
	return &store{
		File: f,                    //el archivo
		size: uint64(stats.Size()), //el tamaño del archivo sacado de stats
		buf:  bufio.NewWriter(f),   //el buffer
	}, err
}

func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	// funcion para leer cuando estamos ya en un archivo
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, offset)
}

func (s *store) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.Remove(s.Name())
}

func (s *store) Close() error { //funcion para cerrar el Archivo

	if err := s.buf.Flush(); err != nil {
		return err //Si hay un error en el flush, entonces devolverlo
	}
	return s.File.Close() //Si no hay error, entonces cerramos el archivo
}
