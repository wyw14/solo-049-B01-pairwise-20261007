package antenna

import (
	"fmt"
	"sync"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type Fleet struct {
	mu     sync.RWMutex
	path   string
	states map[string]State
	audit  *audit.Trail
}

func NewFleet(paths store.Paths, trail *audit.Trail, ids []string) (*Fleet, error) {
	fleet := &Fleet{path: paths.Snapshot("antennas"), states: make(map[string]State), audit: trail}
	var states []State
	found, err := store.ReadJSON(fleet.path, &states)
	if err != nil {
		return nil, err
	}
	if found {
		for _, state := range states {
			fleet.states[state.ID] = state
		}
	}
	for _, id := range ids {
		if _, exists := fleet.states[id]; !exists {
			fleet.states[id] = State{ID: id, Mode: Idle, Azimuth: 180, Elevation: 15}
		}
	}
	if err := fleet.flushLocked(); err != nil {
		return nil, err
	}
	return fleet, nil
}

// ApplyOwner implements scheduler.Ownership. It runs inside the Timeline's
// commit boundary: it only succeeds while the reservation is the live owner
// the timeline is activating, never from a stale snapshot handed in by a
// caller. The fleet's lock is nested under the timeline lock.
func (f *Fleet) ApplyOwner(reservation scheduler.Interval) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[reservation.AntennaID]
	if !found {
		return fmt.Errorf("antenna %s not found", reservation.AntennaID)
	}
	if state.OwnerPassID != "" &&
		(state.OwnerPassID != reservation.PassID || state.OwnerGeneration != reservation.Generation) {
		return fmt.Errorf("antenna %s is owned by pass %s generation %d", state.ID, state.OwnerPassID, state.OwnerGeneration)
	}
	state.OwnerPassID = reservation.PassID
	state.OwnerGeneration = reservation.Generation
	f.states[state.ID] = state
	if err := f.flushLocked(); err != nil {
		return err
	}
	_, _ = f.audit.Record(audit.Event{Component: "antenna", Action: "claimed", Subject: state.ID, Fields: map[string]any{"pass_id": state.OwnerPassID, "generation": state.OwnerGeneration}})
	return nil
}

// ClearOwner implements scheduler.Ownership. It frees the antenna only while
// it is still held by the reservation being retired; if ownership has already
// moved to another pass or a newer generation, the antenna is left untouched.
func (f *Fleet) ClearOwner(reservation scheduler.Interval) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[reservation.AntennaID]
	if !found {
		return fmt.Errorf("antenna %s not found", reservation.AntennaID)
	}
	if state.OwnerPassID != reservation.PassID || state.OwnerGeneration != reservation.Generation {
		return nil
	}
	state.OwnerPassID = ""
	state.OwnerGeneration = 0
	state.Mode = Idle
	f.states[reservation.AntennaID] = state
	if err := f.flushLocked(); err != nil {
		return err
	}
	_, _ = f.audit.Record(audit.Event{Component: "antenna", Action: "released", Subject: state.ID, Fields: map[string]any{"pass_id": reservation.PassID, "generation": reservation.Generation}})
	return nil
}

func (f *Fleet) State(id string) (State, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state, found := f.states[id]
	return state, found
}

func (f *Fleet) List() []State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	values := make([]State, 0, len(f.states))
	for _, state := range f.states {
		values = append(values, state)
	}
	return values
}

func (f *Fleet) update(state State) error {
	f.states[state.ID] = state
	return f.flushLocked()
}

func (f *Fleet) flushLocked() error {
	values := make([]State, 0, len(f.states))
	for _, state := range f.states {
		values = append(values, state)
	}
	return store.WriteJSON(f.path, values)
}
