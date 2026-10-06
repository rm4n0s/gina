// Package prng provides the deterministic random sources used by the simulator:
// SplitMix64 for seeding, xoshiro256** as the generator, and a Tree that derives
// independent child streams from one master seed by domain name.
package prng

import "math/bits"

// SplitMix64 is the seeding generator recommended by the xoshiro authors.
type SplitMix64 struct{ s uint64 }

func NewSplitMix64(seed uint64) *SplitMix64 { return &SplitMix64{s: seed} }

func (r *SplitMix64) Next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Rand is xoshiro256**.
type Rand struct{ s [4]uint64 }

func New(seed uint64) *Rand {
	sm := NewSplitMix64(seed)
	r := &Rand{}
	for i := range r.s {
		r.s[i] = sm.Next()
	}
	return r
}

// FromState builds a generator from an explicit state (used for known-answer tests).
func FromState(s [4]uint64) *Rand { return &Rand{s: s} }

func (r *Rand) Uint64() uint64 {
	result := bits.RotateLeft64(r.s[1]*5, 7) * 9
	t := r.s[1] << 17
	r.s[2] ^= r.s[0]
	r.s[3] ^= r.s[1]
	r.s[1] ^= r.s[2]
	r.s[0] ^= r.s[3]
	r.s[2] ^= t
	r.s[3] = bits.RotateLeft64(r.s[3], 45)
	return result
}

// Uint64n returns a uniform value in [0, n) without modulo bias.
func (r *Rand) Uint64n(n uint64) uint64 {
	if n == 0 {
		panic("prng: Uint64n(0)")
	}
	threshold := -n % n
	for {
		v := r.Uint64()
		if v >= threshold {
			return v % n
		}
	}
}

func (r *Rand) Intn(n int) int { return int(r.Uint64n(uint64(n))) }

// Roll reports true with probability num/den using integer arithmetic only.
func (r *Rand) Roll(num, den uint32) bool {
	if num == 0 || den == 0 {
		return false
	}
	if num >= den {
		return true
	}
	return r.Uint64n(uint64(den)) < uint64(num)
}

// ShuffleU8 is a Fisher-Yates shuffle.
func (r *Rand) ShuffleU8(a []uint8) {
	for i := len(a) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

// Tree derives independent child streams from a master seed. Adding a new child
// name never changes the output of existing children.
type Tree struct{ seed uint64 }

func NewTree(seed uint64) Tree { return Tree{seed: seed} }

func (t Tree) Child(name string) *Rand {
	h := uint64(14695981039346656037)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return New(NewSplitMix64(t.seed ^ h).Next())
}
