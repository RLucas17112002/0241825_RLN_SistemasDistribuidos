package log

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWitdh        = offWidth + posWidth
)

type index struct {
	File *os.File
	mmap gommap.MMap
	size uint64
}

// Funcion para crear un nuevo index
func newIndex(f *os.File, config Config) (*index, error) {

	stats, err := f.Stat() //Se obtiene la informacion para tener el tamaño
	if err != nil {
		f.Close()
		return nil, err //SI hay un error, entonces cerrar el archivo y devolver el error
	}

	f.Truncate(int64(config.Segment.MaxIndexBytes))

	// Hacemos el mapeo de memoria entre el archivo y la memoria
	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err //SI hay error, cerrar el archivo y devolver el valor
	}

	// EN caso de no haber error, entonces devolver el nuevo indice
	return &index{
		File: f,
		mmap: mmap,
		size: uint64(stats.Size()),
	}, nil
}

func (i *index) Read(idx int64) (uint32, uint64, error) {

	//OBtenemos el tamaño del indice
	size := i.size

	//EN caso de tener un tamaño de 0, entonces devolver el final del archivo

	//Sacamos la posicion
	posicion := idx * int64(entWitdh)

	//Checamos si la posicion es mayor al tamaño y si es el caso, devolver el final
	if uint64(posicion) <= size {
		println("Error en index posicion")
		return 0, 0, io.EOF
	}

	//EN caso de ser -1, dividirlo con entWIdth
	if idx == -1 {
		idx = int64(size / entWitdh)
		idx--
	}

	if size == 0 {
		println("Error en index EOF")
		return 0, 0, io.EOF
	}

	//SI no se cumple lo anteriorm entonces, calcular el indice y offset
	offset := enc.Uint32(i.mmap[posicion : posicion+int64(offWidth)])
	indice := enc.Uint64(i.mmap[posicion+int64(offWidth) : posicion+int64(entWitdh)])

	//DEvolver lo obtenido
	return offset, indice, nil
}

func (i *index) Write(offset uint32, posicion uint64) error {
	// Pimero obtenemos el tamaño del index
	size := i.size

	//En caso de sobrepasar el tamaño con el entWidth, significa que alcanzamos el final del archivo
	if (uint64(size) + uint64(entWitdh)) > uint64(len(i.mmap)) {
		return io.EOF
	}

	// Escribimos la posicion desde el final del offset hasta el final de la posicion
	binary.BigEndian.PutUint32(i.mmap[size:], offset)
	binary.BigEndian.PutUint64(i.mmap[size+uint64(offWidth):], posicion)
	size += uint64(entWitdh)

	return nil

}

// FUncion que da el nombre del archivo
func (i *index) Name() string {
	return i.File.Name()
}

// Funcion para quitar
func (i *index) Remove() error {
	if err := os.Remove(i.File.Name()); err != nil {
		return err
	}
	i.File.Truncate(int64(i.size))
	if err := i.File.Close(); err != nil {
		return err
	}
	return nil
}

// Cerramos el archivo
func (i *index) Close() error {
	// CHecamos memoria a ver si hay flush
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	//Truncamos el archivo
	i.File.Truncate(int64(i.size))

	//Cerramos el archivo y en caso de haber error, entonces devolverlo
	if err := i.File.Close(); err != nil {
		return err
	}
	return nil
}
