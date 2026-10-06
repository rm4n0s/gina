package lint

import "testing"

func TestRules(t *testing.T) {
	cases := []struct {
		name, src string
		want      int
	}{
		{"clean", "package p\nfunc f() int { return 1 }", 0},
		{"go", "package p\nfunc f() { go g() }\nfunc g() {}", 1},
		{"chan type", "package p\nvar c chan int", 1},
		{"make chan send recv", "package p\nfunc f() { c := make(chan int, 1); c <- 1; <-c }", 3},
		{"select", "package p\nfunc f(a, b int) { select {} }", 1},
		{"os/signal", "package p\nimport _ \"os/signal\"", 1},
		{"context", "package p\nimport _ \"context\"", 1},
		{"time.After", "package p\nimport \"time\"\nfunc f() { _ = time.After(1) }", 1},
		{"time.Sleep ok", "package p\nimport \"time\"\nfunc f() { time.Sleep(1) }", 0},
		{"waitgroup", "package p\nimport \"sync\"\nvar w sync.WaitGroup", 1},
		{"parallel", "package p\nfunc f(t T) { t.Parallel() }", 1},
	}
	for _, c := range cases {
		got, err := CheckSource(c.name+".go", c.src)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(got) != c.want {
			t.Errorf("%s: got %d findings %v, want %d", c.name, len(got), got, c.want)
		}
	}
}
