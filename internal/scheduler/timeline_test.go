package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type fakeOwner struct {
	owner   map[string]Interval
	failOn  map[string]bool
	applied int
	cleared int
}

func newFakeOwner() *fakeOwner {
	return &fakeOwner{owner: make(map[string]Interval), failOn: make(map[string]bool)}
}

func (f *fakeOwner) ApplyOwner(reservation Interval) error {
	if f.failOn[reservation.ReservationID] {
		return errors.New("ownership refused")
	}
	current, ok := f.owner[reservation.AntennaID]
	if ok && (current.PassID != reservation.PassID || current.Generation != reservation.Generation) {
		return fmt.Errorf("antenna %s held by pass %s", reservation.AntennaID, current.PassID)
	}
	f.owner[reservation.AntennaID] = reservation
	f.applied++
	return nil
}

func (f *fakeOwner) ClearOwner(reservation Interval) error {
	current, ok := f.owner[reservation.AntennaID]
	if ok && current.PassID == reservation.PassID && current.Generation == reservation.Generation {
		delete(f.owner, reservation.AntennaID)
		f.cleared++
	}
	return nil
}

func newTestTimeline(t *testing.T) (*Timeline, *fakeOwner) {
	t.Helper()
	timeline, err := NewTimeline(store.NewPaths(t.TempDir()), audit.NewTrail(store.NewAppendLog(store.NewPaths(t.TempDir()))))
	if err != nil {
		t.Fatal(err)
	}
	owner := newFakeOwner()
	timeline.RegisterOwnership(owner)
	return timeline, owner
}

func reserveWindow(t *testing.T, timeline *Timeline, passID, antennaID string, start, end time.Time) Interval {
	t.Helper()
	interval, err := timeline.Reserve(passID, antennaID, start, end)
	if err != nil {
		t.Fatal(err)
	}
	return interval
}

func TestClaimRejectsReleasedAndExpiredReservations(t *testing.T) {
	timeline, owner := newTestTimeline(t)
	now := time.Now().UTC()
	future := reserveWindow(t, timeline, "pass-live", "ANT-01", now.Add(time.Minute), now.Add(9*time.Minute))
	past := reserveWindow(t, timeline, "pass-old", "ANT-02", now.Add(-2*time.Minute), now.Add(-time.Minute))

	if _, err := timeline.Claim(past.ReservationID, now); err == nil {
		t.Fatal("claim of an expired-window reservation unexpectedly succeeded")
	}
	if _, held := owner.owner["ANT-02"]; held {
		t.Fatal("expired reservation occupied antenna ANT-02")
	}

	if _, err := timeline.Claim(future.ReservationID, now); err != nil {
		t.Fatalf("claim before window end unexpectedly failed: %v", err)
	}
	released, err := timeline.Release(future.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if released.Status != Released {
		t.Fatalf("release set status %s", released.Status)
	}
	if _, held := owner.owner["ANT-01"]; held {
		t.Fatal("released reservation still occupies antenna ANT-01")
	}
	if _, err := timeline.Claim(future.ReservationID, now); err == nil {
		t.Fatal("reclaim of a released reservation unexpectedly succeeded")
	}
	if _, held := owner.owner["ANT-01"]; held {
		t.Fatal("released reservation re-occupied antenna ANT-01")
	}
}

func TestExpireAndSweepFreeAntennaAndBlockClaims(t *testing.T) {
	timeline, owner := newTestTimeline(t)
	now := time.Now().UTC()
	old := reserveWindow(t, timeline, "pass-old", "ANT-01", now.Add(-3*time.Minute), now.Add(-time.Minute))
	live := reserveWindow(t, timeline, "pass-live", "ANT-02", now.Add(-time.Minute), now.Add(time.Minute))

	if _, err := timeline.Claim(old.ReservationID, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("historical claim within window failed: %v", err)
	}
	if _, err := timeline.Claim(live.ReservationID, now); err != nil {
		t.Fatalf("live claim failed: %v", err)
	}

	expired := timeline.ExpireDue(now)
	if len(expired) != 1 || expired[0].ReservationID != old.ReservationID {
		t.Fatalf("sweep expired %#v", expired)
	}
	if _, held := owner.owner["ANT-01"]; held {
		t.Fatal("swept reservation still occupies antenna ANT-01")
	}
	if _, held := owner.owner["ANT-02"]; !held {
		t.Fatal("sweep released a reservation still inside its window")
	}

	explicit, err := timeline.Expire(old.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if explicit.Status != Expired {
		t.Fatalf("expire set status %s", explicit.Status)
	}
	if _, err := timeline.Claim(old.ReservationID, now); err == nil {
		t.Fatal("claim of an expired reservation unexpectedly succeeded")
	}
	if current, ok := timeline.CurrentOwner("ANT-01", now.Add(-90*time.Second)); ok {
		t.Fatalf("expired reservation reported as current owner: %#v", current)
	}
	if _, err := timeline.Extend(old.ReservationID, now.Add(time.Hour)); err == nil {
		t.Fatal("extend of an expired reservation unexpectedly succeeded")
	}
}

func TestTerminalReservationFreesSlotForReplacement(t *testing.T) {
	timeline, owner := newTestTimeline(t)
	now := time.Now().UTC()
	first := reserveWindow(t, timeline, "pass-a", "ANT-01", now.Add(-time.Minute), now.Add(time.Minute))
	if _, err := timeline.Claim(first.ReservationID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Reserve("pass-b", "ANT-01", now.Add(-30*time.Second), now.Add(30*time.Second)); err == nil {
		t.Fatal("overlapping live reservation unexpectedly accepted")
	}
	if _, err := timeline.Release(first.ReservationID); err != nil {
		t.Fatal(err)
	}
	if _, held := owner.owner["ANT-01"]; held {
		t.Fatal("released reservation still occupies antenna ANT-01")
	}
	replacement := reserveWindow(t, timeline, "pass-b", "ANT-01", now.Add(-30*time.Second), now.Add(30*time.Second))
	if replacement.Status != Reserved {
		t.Fatalf("replacement status %s", replacement.Status)
	}
}

func TestClaimRollsBackWhenOwnershipRejected(t *testing.T) {
	timeline, owner := newTestTimeline(t)
	now := time.Now().UTC()
	holder := reserveWindow(t, timeline, "pass-holder", "ANT-01", now.Add(-time.Minute), now.Add(time.Minute))
	if _, err := timeline.Claim(holder.ReservationID, now); err != nil {
		t.Fatal(err)
	}
	other := reserveWindow(t, timeline, "pass-other", "ANT-02", now.Add(-time.Minute), now.Add(time.Minute))
	owner.failOn[other.ReservationID] = true
	if _, err := timeline.Claim(other.ReservationID, now); err == nil {
		t.Fatal("claim with failing ownership commit unexpectedly succeeded")
	}
	var persisted []Interval
	if _, err := store.ReadJSON(timeline.path, &persisted); err != nil {
		t.Fatal(err)
	}
	for _, interval := range persisted {
		if interval.ReservationID == other.ReservationID && interval.Status == Active {
			t.Fatal("reservation was persisted active after ownership commit failed")
		}
	}
	for _, interval := range timeline.List("ANT-02") {
		if interval.ReservationID == other.ReservationID && interval.Status == Active {
			t.Fatal("reservation stayed active in memory after ownership commit failed")
		}
	}
}
