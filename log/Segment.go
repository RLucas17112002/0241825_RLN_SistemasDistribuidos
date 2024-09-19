package log

import (
	"fmt"
	"os"
	"path"

	log_v1 "github.com/Lucas/api/v1"

	"google.golang.org/protobuf/proto"
)

type Segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func NewSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	s := &Segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *Segment) Append(record *log_v1.Record) (uint64, error) {
	offset := s.nextOffset
	record.Offset = offset

	content, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, posicion, err := s.store.Append(content)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)), posicion); err != nil {
		return 0, err
	}

	s.nextOffset++
	return offset, nil
}

func (s *Segment) Read(offset uint64) (*log_v1.Record, error) {

	_, posicion, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		println("Error en peticion")
		return nil, err
	}

	record := &log_v1.Record{}
	record.Offset = offset
	content, err := s.store.Read(posicion)

	if err != nil {
		println("Error en el store Read")
		return nil, err
	}

	if err = proto.Unmarshal(content, record); err != nil {
		println("Error en el marshal")
		return nil, err
	}

	return record, err
}

func (s *Segment) IsMaxed() bool {
	isMaxStore := s.store.size >= s.config.Segment.MaxStoresBytes
	isMaxIndex := s.index.size >= s.config.Segment.MaxIndexBytes

	return isMaxIndex || isMaxStore
}

func (s *Segment) Remove() error {
	if err := s.index.Remove(); err != nil {
		return err
	}
	if err := s.store.Remove(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Name() string {
	return fmt.Sprintf("%d - %d", s.baseOffset, s.nextOffset)
}
