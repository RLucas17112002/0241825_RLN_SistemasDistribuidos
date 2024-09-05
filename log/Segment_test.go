package log

import (
	"io"
	"os"
	"testing"

	log_v1 "github.com/Lucas/api/v1"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "Segment-test")
	defer os.RemoveAll(dir)

	want := &log_v1.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoresBytes = 1024
	c.Segment.MaxIndexBytes = entWitdh * 3

	s, err := NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	require.True(t, s.IsMaxed())

	c.Segment.MaxStoresBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
