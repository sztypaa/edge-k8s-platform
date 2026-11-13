package model

import "testing"

func TestScore(t *testing.T) {
	task := TaskCFP{MinCPUMilli: 500, MinMemMiB: 256}

	empty := Score(4000, 4096, 4000, 4096, false, task)
	cached := Score(2000, 2048, 4000, 4096, true, task)
	if cached <= empty {
		t.Errorf("half-loaded cluster with cached image (%d) should beat empty cluster without it (%d)", cached, empty)
	}

	almostFull := Score(600, 300, 4000, 4096, true, task)
	if almostFull >= empty {
		t.Errorf("almost full cluster with cached image (%d) should lose to empty cluster (%d)", almostFull, empty)
	}

	rpi := Score(2000, 2048, 4000, 4096, false, task)
	vm := Score(2000, 2048, 8000, 16384, false, task)
	if rpi <= vm {
		t.Errorf("same free resources are a bigger share of a smaller cluster: rpi=%d vm=%d", rpi, vm)
	}

	if got := Score(4000, 4096, 0, 0, true, task); got != 0 {
		t.Errorf("missing totals should give score 0, got %d", got)
	}
}
