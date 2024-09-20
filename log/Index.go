package log

import (
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

	idx := &index{
		File: f,
	}

	stats, err := f.Stat() //Se obtiene la informacion para tener el tamaño
	if err != nil {
		return nil, err //SI hay un error, entonces cerrar el archivo y devolver el error
	}

	idx.size = uint64(stats.Size())

	if err = os.Truncate(
		f.Name(), int64(config.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// Hacemos el mapeo de memoria entre el archivo y la memoria

	if idx.mmap, err = gommap.Map(
		idx.File.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Write(offset uint32, posicion uint64) error {

	//En caso de sobrepasar el tamaño con el entWidth, significa que alcanzamos el final del archivo
	if (i.size + entWitdh) > uint64(len(i.mmap)) {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWitdh], posicion)
	// Escribimos la posicion desde el final del offset hasta el final de la posicion

	i.size += uint64(entWitdh)

	return nil

}

func (i *index) Read(idx int64) (out uint32, posicion uint64, err error) {

	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if idx == -1 {
		out = uint32((i.size / entWitdh) - 1)
	} else {
		out = uint32(idx)
	}

	//EN caso de tener un tamaño de 0, entonces devolver el final del archivo

	//Sacamos la posicion
	posicion = uint64(out) * entWitdh
	if i.size < posicion+entWitdh {
		return 0, 0, io.EOF
	}

	//SI no se cumple lo anteriorm entonces, calcular el indice y offset
	out = enc.Uint32(i.mmap[posicion : posicion+(offWidth)])
	posicion = enc.Uint64(i.mmap[posicion+(offWidth) : posicion+(entWitdh)])

	//DEvolver lo obtenido
	return out, posicion, nil
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
