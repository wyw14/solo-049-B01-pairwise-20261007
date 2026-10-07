package antenna

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

func newTestFleet(t *testing.T, now time.Time) (*Fleet, *scheduler.Timeline) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	if err := paths.Validate(); err != nil {
		t.Fatal(err)
	}
	trail := audit.NewTrail(store.NewAppendLog(paths))
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewFleet(paths, timeline, trail, []string{"ANT-01"})
	if err != nil {
		t.Fatal(err)
	}
	fleet.SetClock(func() time.Time { return now })
	return fleet, timeline
}

func TestClaimCommitsOwnershipForValidReservation(t *testing.T) {
	now := time.Now().UTC()
	fleet, timeline := newTestFleet(t, now)
	reservation, err := timeline.Reserve("PASS-1", "ANT-01", now.Add(-time.Minute), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	state, err := fleet.Claim(reservation)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if state.OwnerPassID != "PASS-1" || state.OwnerGeneration != reservation.Generation {
		t.Fatalf("ownership not committed: %#v", state)
	}
}

func TestClaimRejectsReleasedReservation(t *testing.T) {
	now := time.Now().UTC()
	fleet, timeline := newTestFleet(t, now)
	reservation, err := timeline.Reserve("PASS-1", "ANT-01", now.Add(-time.Minute), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Release(reservation.ReservationID); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Claim(reservation); err == nil {
		t.Fatal("claim with released reservation unexpectedly succeeded")
	}
	state, found := fleet.State("ANT-01")
	if !found || state.OwnerPassID != "" {
		t.Fatalf("released reservation occupied antenna: %#v", state)
	}
}

func TestClaimRejectsExpiredReservation(t *testing.T) {
	now := time.Now().UTC()
	fleet, timeline := newTestFleet(t, now)
	reservation, err := timeline.Reserve("PASS-1", "ANT-01", now.Add(-10*time.Minute), now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Claim(reservation); err == nil {
		t.Fatal("claim with expired reservation unexpectedly succeeded")
	}
	state, found := fleet.State("ANT-01")
	if !found || state.OwnerPassID != "" {
		t.Fatalf("expired reservation occupied antenna: %#v", state)
	}
}

func TestClaimRejectsStaleGeneration(t *testing.T) {
	now := time.Now().UTC()
	fleet, timeline := newTestFleet(t, now)
	stale, err := timeline.Reserve("PASS-1", "ANT-01", now.Add(-time.Minute), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Release(stale.ReservationID); err != nil {
		t.Fatal(err)
	}
	current, err := timeline.Reserve("PASS-1", "ANT-01", now.Add(-time.Minute), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Claim(stale); err == nil {
		t.Fatal("claim with superseded reservation unexpectedly succeeded")
	}
	if _, err := fleet.Claim(current); err != nil {
		t.Fatalf("claim with current reservation failed: %v", err)
	}
	state, _ := fleet.State("ANT-01")
	if state.OwnerGeneration != current.Generation {
		t.Fatalf("expected generation %d, got %d", current.Generation, state.OwnerGeneration)
	}
}
