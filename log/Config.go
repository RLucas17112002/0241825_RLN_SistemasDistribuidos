package log

type Config struct {
	Segment struct {
		MaxStoresBytes uint64
		MaxIndexBytes  uint64
		InitialOffset  uint64
	}
}
