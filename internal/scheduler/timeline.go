package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

// Ownership commits antenna ownership together with a reservation transition.
// It is implemented by the antenna fleet and invoked while the Timeline lock
// is held, so reservation status and antenna ownership publish as one boundary.
type Ownership interface {
	// ApplyOwner records the reservation as the antenna owner. It must reject
	// the change when the antenna is held by a different pass or generation.
	ApplyOwner(reservation Interval) error
	// ClearOwner releases the antenna only while it is still held by the given
	// reservation. When ownership has already moved on it is a no-op.
	ClearOwner(reservation Interval) error
}

// Timeline owns the check-and-publish boundary for every antenna mutation.
type Timeline struct {
	mu        sync.RWMutex
	path      string
	byAntenna map[string][]Interval
	nextGen   map[string]int64
	ownership Ownership
	audit     *audit.Trail
}

func NewTimeline(paths store.Paths, trail *audit.Trail) (*Timeline, error) {
	timeline := &Timeline{
		path: paths.Snapshot("antenna-timeline"), byAntenna: make(map[string][]Interval),
		nextGen: make(map[string]int64), audit: trail,
	}
	var values []Interval
	found, err := store.ReadJSON(timeline.path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			timeline.byAntenna[value.AntennaID] = append(timeline.byAntenna[value.AntennaID], value)
			if value.Generation > timeline.nextGen[value.AntennaID] {
				timeline.nextGen[value.AntennaID] = value.Generation
			}
		}
	}
	return timeline, nil
}

// RegisterOwnership wires the fleet that must commit antenna ownership in the
// same boundary as reservation transitions.
func (t *Timeline) RegisterOwnership(ownership Ownership) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ownership = ownership
}

func (t *Timeline) Reserve(passID, antennaID string, start, end time.Time) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextGen[antennaID]++
	value := Interval{
		ReservationID: uuid.NewString(), PassID: passID, AntennaID: antennaID,
		Start: start, End: end, Generation: t.nextGen[antennaID], Status: Reserved,
	}
	if err := value.Validate(); err != nil {
		return Interval{}, err
	}
	if conflict := t.conflictLocked(value, ""); conflict != nil {
		return Interval{}, conflict
	}
	t.byAntenna[antennaID] = append(t.byAntenna[antennaID], value)
	if err := t.flushLocked(); err != nil {
		t.byAntenna[antennaID] = t.byAntenna[antennaID][:len(t.byAntenna[antennaID])-1]
		return Interval{}, err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: "reserved", Subject: value.ReservationID, Fields: map[string]any{"pass_id": passID, "antenna_id": antennaID, "generation": value.Generation}})
	return value, nil
}

func (t *Timeline) Extend(reservationID string, newEnd time.Time) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	antennaID, index, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if current.Status.Terminal() {
		return Interval{}, fmt.Errorf("reservation %s is %s and cannot be extended", reservationID, current.Status)
	}
	updated := current
	updated.End = newEnd
	if err := updated.Validate(); err != nil {
		return Interval{}, err
	}
	if conflict := t.conflictLocked(updated, reservationID); conflict != nil {
		return Interval{}, conflict
	}
	t.byAntenna[antennaID][index] = updated
	if err := t.flushLocked(); err != nil {
		t.byAntenna[antennaID][index] = current
		return Interval{}, err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: "extended", Subject: reservationID, Fields: map[string]any{"end": newEnd}})
	return updated, nil
}

// Claim activates a reservation and transfers antenna ownership to it in one
// boundary. Released or expired reservations, and reservations whose window
// has already ended, can never claim an antenna.
func (t *Timeline) Claim(reservationID string, at time.Time) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	antennaID, index, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if current.Status.Terminal() {
		return Interval{}, fmt.Errorf("reservation %s is %s and cannot claim antenna %s", reservationID, current.Status, antennaID)
	}
	if !at.Before(current.End) {
		return Interval{}, fmt.Errorf("reservation %s expired at %s and cannot claim antenna %s", reservationID, current.End.Format(time.RFC3339), antennaID)
	}
	updated := current
	updated.Status = Active
	if err := t.transitionLocked(antennaID, index, current, updated); err != nil {
		return Interval{}, err
	}
	return updated, nil
}

// Release retires a reservation and frees its antenna in the same boundary.
// It is idempotent: an already terminal reservation is returned unchanged.
func (t *Timeline) Release(reservationID string) (Interval, error) {
	return t.terminate(reservationID, Released)
}

// Expire marks a reservation whose window has ended as expired and frees its
// antenna. It is idempotent for terminal reservations.
func (t *Timeline) Expire(reservationID string) (Interval, error) {
	return t.terminate(reservationID, Expired)
}

func (t *Timeline) terminate(reservationID string, status ReservationStatus) (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	antennaID, index, current, found := t.findLocked(reservationID)
	if !found {
		return Interval{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if current.Status.Terminal() {
		return current, nil
	}
	updated := current
	updated.Status = status
	if err := t.transitionLocked(antennaID, index, current, updated); err != nil {
		return Interval{}, err
	}
	return updated, nil
}

// ExpireDue retires every non-terminal reservation whose window has ended at
// the given instant, releasing any antenna it still holds. Reservations whose
// ownership could not be released are left live so the antenna stays visibly
// owned and a later sweep can retry.
func (t *Timeline) ExpireDue(at time.Time) []Interval {
	t.mu.Lock()
	defer t.mu.Unlock()
	var expired []Interval
	for antennaID := range t.byAntenna {
		for index := range t.byAntenna[antennaID] {
			current := t.byAntenna[antennaID][index]
			if current.Status.Terminal() || at.Before(current.End) {
				continue
			}
			updated := current
			updated.Status = Expired
			if err := t.transitionLocked(antennaID, index, current, updated); err != nil {
				_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: "expire.failed", Subject: current.ReservationID, Severity: audit.Warning, Fields: map[string]any{"pass_id": current.PassID, "error": err.Error()}})
				continue
			}
			expired = append(expired, updated)
		}
	}
	return expired
}

func (t *Timeline) List(antennaID string) []Interval {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var values []Interval
	if antennaID != "" {
		values = append(values, t.byAntenna[antennaID]...)
	} else {
		for _, intervals := range t.byAntenna {
			values = append(values, intervals...)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Start.Before(values[j].Start) })
	return values
}

func (t *Timeline) CurrentOwner(antennaID string, at time.Time) (Interval, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, value := range t.byAntenna[antennaID] {
		if !value.Status.Terminal() && !at.Before(value.Start) && at.Before(value.End) {
			return value, true
		}
	}
	return Interval{}, false
}

// transitionLocked updates a reservation and keeps antenna ownership in the
// same commit: an active reservation applies ownership, a terminal one clears
// it. Any failure restores both the reservation and its previous ownership.
func (t *Timeline) transitionLocked(antennaID string, index int, previous, updated Interval) error {
	forward, reverse := t.ownerStepsLocked(previous, updated)
	t.byAntenna[antennaID][index] = updated
	if t.ownership != nil && forward != nil {
		if err := forward(updated); err != nil {
			t.byAntenna[antennaID][index] = previous
			return err
		}
	}
	if err := t.flushLocked(); err != nil {
		if t.ownership != nil && reverse != nil {
			_ = reverse(previous)
		}
		t.byAntenna[antennaID][index] = previous
		return err
	}
	_, _ = t.audit.Record(audit.Event{Component: "scheduler", Action: string(updated.Status), Subject: updated.ReservationID, Fields: map[string]any{"pass_id": updated.PassID, "antenna_id": updated.AntennaID, "generation": updated.Generation}})
	return nil
}

// ownerStepsLocked returns the ownership change for a transition and the
// change that undoes it when the timeline flush fails.
func (t *Timeline) ownerStepsLocked(previous, updated Interval) (forward, reverse func(Interval) error) {
	if t.ownership == nil {
		return nil, nil
	}
	switch {
	case updated.Status == Active && previous.Status != Active:
		return t.ownership.ApplyOwner, t.ownership.ClearOwner
	case updated.Status.Terminal() && !previous.Status.Terminal():
		return t.ownership.ClearOwner, t.ownership.ApplyOwner
	default:
		return nil, nil
	}
}

func (t *Timeline) conflictLocked(candidate Interval, excluded string) error {
	for _, existing := range t.byAntenna[candidate.AntennaID] {
		if existing.ReservationID == excluded || existing.Status.Terminal() {
			continue
		}
		if candidate.Overlaps(existing) {
			return &ConflictError{AntennaID: candidate.AntennaID, Existing: existing}
		}
	}
	return nil
}

func (t *Timeline) findLocked(id string) (string, int, Interval, bool) {
	for antennaID, intervals := range t.byAntenna {
		for index, value := range intervals {
			if value.ReservationID == id {
				return antennaID, index, value, true
			}
		}
	}
	return "", 0, Interval{}, false
}

func (t *Timeline) flushLocked() error {
	var values []Interval
	for _, intervals := range t.byAntenna {
		values = append(values, intervals...)
	}
	return store.WriteJSON(t.path, values)
}
