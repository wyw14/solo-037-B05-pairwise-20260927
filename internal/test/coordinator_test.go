package test

import (
    "testing"
    "time"

    "github.com/wyw14/solo-037/internal/force"
    "github.com/wyw14/solo-037/internal/journal"
    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/probe"
)

func newCoordinator(t *testing.T, store *journal.Store, at time.Time) *Coordinator {
    t.Helper()
    operation := model.NewOperation("run", "hull", "section-a", "carriage-1", at)
    return NewCoordinator(operation, store, probe.NewCollection(), force.NewReceiver())
}

func TestArmFailureKeepsStage(t *testing.T) {
    at := time.Unix(1700000000, 0)
    store, err := journal.NewStore(t.TempDir())
    if err != nil {
        t.Fatal(err)
    }
    coordinator := newCoordinator(t, store, at)

    store.FailNext("stage")
    if err := coordinator.Arm(at); err == nil {
        t.Fatal("expected append error")
    }
    if got := coordinator.Snapshot().Stage; got != model.StageConfigured {
        t.Fatalf("stage advanced on failed append: %s", got)
    }
    if got := Restore(store.Records(), coordinator.Snapshot().ID).Stage; got != model.StageConfigured {
        t.Fatalf("replay after failed append: %s", got)
    }
}

func TestArmPersistsBeforeStateChange(t *testing.T) {
    at := time.Unix(1700000000, 0)
    store, err := journal.NewStore(t.TempDir())
    if err != nil {
        t.Fatal(err)
    }
    coordinator := newCoordinator(t, store, at)

    if err := coordinator.Arm(at); err != nil {
        t.Fatal(err)
    }
    if got := coordinator.Snapshot().Stage; got != model.StageArming {
        t.Fatalf("stage after arm: %s", got)
    }
    restored := Restore(store.Records(), coordinator.Snapshot().ID)
    if restored.Stage != model.StageArming {
        t.Fatalf("replay after restart: %s", restored.Stage)
    }
}

func TestBeginMeasurementFailureKeepsStage(t *testing.T) {
    at := time.Unix(1700000000, 0)
    store, err := journal.NewStore(t.TempDir())
    if err != nil {
        t.Fatal(err)
    }
    coordinator := newCoordinator(t, store, at)
    if err := coordinator.Arm(at); err != nil {
        t.Fatal(err)
    }

    store.FailNext("stage")
    if err := coordinator.BeginMeasurement(at); err == nil {
        t.Fatal("expected append error")
    }
    if got := coordinator.Snapshot().Stage; got != model.StageArming {
        t.Fatalf("stage advanced on failed append: %s", got)
    }
    if got := Restore(store.Records(), coordinator.Snapshot().ID).Stage; got != model.StageArming {
        t.Fatalf("replay after failed append: %s", got)
    }

    if err := coordinator.BeginMeasurement(at); err != nil {
        t.Fatal(err)
    }
    if got := coordinator.Snapshot().Stage; got != model.StageMeasuring {
        t.Fatalf("stage after retry: %s", got)
    }
    if got := Restore(store.Records(), coordinator.Snapshot().ID).Stage; got != model.StageMeasuring {
        t.Fatalf("replay after retry: %s", got)
    }
}
