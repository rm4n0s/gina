package prng

import "testing"

func TestSplitMix64KnownAnswer(t *testing.T) {
	r := NewSplitMix64(0)
	if got := r.Next(); got != 0xe220a8397b1dcdaf {
		t.Fatalf("splitmix64(0) first = %#x", got)
	}
}

func TestXoshiro256StarStarKnownAnswer(t *testing.T) {
	r := FromState([4]uint64{1, 2, 3, 4})
	want := []uint64{11520, 0, 1509978240, 1215971899390074240}
	for i, w := range want {
		if got := r.Uint64(); got != w {
			t.Fatalf("output %d = %d, want %d", i, got, w)
		}
	}
}

func TestChildIndependence(t *testing.T) {
	a := NewTree(42).Child("net")
	_ = NewTree(42).Child("something-new")
	b := NewTree(42).Child("net")
	for i := 0; i < 100; i++ {
		if a.Uint64() != b.Uint64() {
			t.Fatal("child stream changed")
		}
	}
	if NewTree(42).Child("a").Uint64() == NewTree(42).Child("b").Uint64() {
		t.Fatal("different children produced the same first value")
	}
}

func TestUint64nRangeAndRoll(t *testing.T) {
	r := New(1)
	for i := 0; i < 1000; i++ {
		if v := r.Uint64n(7); v >= 7 {
			t.Fatalf("out of range: %d", v)
		}
	}
	if r.Roll(0, 10) || !r.Roll(10, 10) {
		t.Fatal("roll edge cases")
	}
}
