package test

import (
	"testing"
	"time"

	"github.com/wyw14/solo-037/internal/force"
	"github.com/wyw14/solo-037/internal/journal"
	"github.com/wyw14/solo-037/internal/model"
	"github.com/wyw14/solo-037/internal/probe"
)

func newTestCoordinator(t *testing.T) (*Coordinator, *journal.Store, model.Operation) {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	operation := model.NewOperation("water-run", "hull", "section-a", "carriage-1", time.Unix(1700000000, 0))
	return NewCoordinator(operation, store, probe.NewCollection(), force.NewReceiver()), store, operation
}

// A failed stage-event append must not advance the in-memory stage: the run
// stays in its original stage and the transition can be retried.
func TestArmKeepsOriginalStageWhenJournalAppendFails(t *testing.T) {
	coordinator, store, _ := newTestCoordinator(t)
	at := time.Unix(1700000001, 0)

	store.FailNext("stage")
	if err := coordinator.Arm(at); err == nil {
		t.Fatal("expected Arm to return the journal append error")
	}

	if stage := coordinator.Snapshot().Stage; stage != model.StageConfigured {
		t.Fatalf("stage changed despite failed append: got %s want %s", stage, model.StageConfigured)
	}
	if records := store.Records(); len(records) != 0 {
		t.Fatalf("failed append left %d records, want none", len(records))
	}

	// Retry now that the injected failure is cleared: it must succeed.
	if err := coordinator.Arm(at); err != nil {
		t.Fatalf("retry after failed append: %v", err)
	}
	if stage := coordinator.Snapshot().Stage; stage != model.StageArming {
		t.Fatalf("stage after successful retry: got %s want %s", stage, model.StageArming)
	}
}

// A failed append during BeginMeasurement (which spans accelerating ->
// measuring) must leave the run in arming rather than half-advanced.
func TestBeginMeasurementKeepsOriginalStageWhenJournalAppendFails(t *testing.T) {
	coordinator, store, _ := newTestCoordinator(t)
	at := time.Unix(1700000001, 0)
	if err := coordinator.Arm(at); err != nil {
		t.Fatal(err)
	}

	store.FailNext("stage")
	if err := coordinator.BeginMeasurement(at); err == nil {
		t.Fatal("expected BeginMeasurement to return the journal append error")
	}
	if stage := coordinator.Snapshot().Stage; stage != model.StageArming {
		t.Fatalf("stage changed despite failed append: got %s want %s", stage, model.StageArming)
	}

	if err := coordinator.BeginMeasurement(at); err != nil {
		t.Fatalf("retry after failed append: %v", err)
	}
	if stage := coordinator.Snapshot().Stage; stage != model.StageMeasuring {
		t.Fatalf("stage after successful retry: got %s want %s", stage, model.StageMeasuring)
	}
}

// Successfully committed stage records must survive a restart and replay to
// the same stage.
func TestCommittedStagesReplayAfterRestart(t *testing.T) {
	directory := t.TempDir()
	store, err := journal.NewStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	operation := model.NewOperation("water-run", "hull", "section-a", "carriage-1", time.Unix(1700000000, 0))
	coordinator := NewCoordinator(operation, store, probe.NewCollection(), force.NewReceiver())
	at := time.Unix(1700000001, 0)
	if err := coordinator.Arm(at); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.BeginMeasurement(at); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart by reopening the store from the same
	// directory and replaying the committed records.
	reopened, err := journal.NewStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	replayed := journal.Replay(reopened.Records(), operation.ID)
	if replayed.Stage != model.StageMeasuring {
		t.Fatalf("replayed stage after restart: got %s want %s", replayed.Stage, model.StageMeasuring)
	}
}
