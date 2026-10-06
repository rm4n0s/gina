package gina

// slab is a chunked, typed slot store. Chunks are allocated on first use (or
// eagerly) and never move or shrink, so *T stays valid for the shard's lifetime.
type slab[T any] struct {
	chunks    [][]T
	chunkSize int
	shift     uint
	mask      uint32
}

func newSlab[T any](slots, chunkSize int, eager bool) *slab[T] {
	s := &slab[T]{chunkSize: chunkSize, mask: uint32(chunkSize - 1)}
	for 1<<s.shift < chunkSize {
		s.shift++
	}
	s.chunks = make([][]T, (slots+chunkSize-1)/chunkSize)
	if eager {
		for i := range s.chunks {
			s.chunks[i] = make([]T, chunkSize)
		}
	}
	return s
}

func (s *slab[T]) at(i uint32) *T {
	c := s.chunks[i>>s.shift]
	if c == nil {
		c = make([]T, s.chunkSize)
		s.chunks[i>>s.shift] = c
	}
	return &c[i&s.mask]
}

// zero resets a slot so the GC can reclaim everything the isolate referenced.
func (s *slab[T]) zero(i uint32) {
	var z T
	*s.at(i) = z
}
