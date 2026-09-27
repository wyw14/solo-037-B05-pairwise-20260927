package test

import (
    "context"
    "errors"
    "sync"
    "time"

    "github.com/wyw14/solo-037/internal/force"
    "github.com/wyw14/solo-037/internal/journal"
    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/probe"
)

type Coordinator struct {
    mu sync.Mutex
    Operation model.Operation
    Context context.Context
    Cancel context.CancelFunc
    Store *journal.Store
    Probes *probe.Collection
    Forces *force.Receiver
}

func NewCoordinator(operation model.Operation, store *journal.Store, probes *probe.Collection, forces *force.Receiver) *Coordinator {
    ctx, cancel := context.WithCancel(context.Background())
    return &Coordinator{Operation: operation, Context: ctx, Cancel: cancel, Store: store, Probes: probes, Forces: forces}
}

func (c *Coordinator) Arm(at time.Time) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    next, err := c.Operation.Advance(model.StageArming, at)
    if err != nil {
        return err
    }
    // Commit the stage event to the write-ahead journal before publishing the
    // new stage in memory. A failed append must leave the run in its original
    // stage so the transition can be retried.
    record, err := journal.StageRecord(next.ID, next.Stage, at)
    if err != nil {
        return err
    }
    if err := c.Store.Append(record); err != nil {
        return err
    }
    c.Operation = next
    return nil
}

func (c *Coordinator) BeginMeasurement(at time.Time) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    next := c.Operation
    if next.Stage == model.StageArming {
        advanced, err := next.Advance(model.StageAccelerating, at)
        if err != nil {
            return err
        }
        next = advanced
    }
    advanced, err := next.Advance(model.StageMeasuring, at)
    if err != nil {
        return err
    }
    next = advanced
    // Publish the new stage only after the stage event is durable. On append
    // failure c.Operation is left untouched (still arming/configured) so the
    // transition can be retried.
    record, err := journal.StageRecord(next.ID, next.Stage, at)
    if err != nil {
        return err
    }
    if err := c.Store.Append(record); err != nil {
        return err
    }
    c.Operation = next
    return nil
}

func (c *Coordinator) Complete(at time.Time) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    if !c.Probes.Durable(c.Operation.ID) || !c.Forces.Durable(c.Operation.ID) {
        return errors.New("measurement evidence is not durable")
    }
    next, err := c.Operation.Advance(model.StageDecelerating, at)
    if err != nil {
        return err
    }
    next, err = next.Advance(model.StageSettling, at)
    if err != nil {
        return err
    }
    next, err = next.Advance(model.StageComplete, at)
    if err != nil {
        return err
    }
    record, err := journal.StageRecord(next.ID, model.StageComplete, at)
    if err != nil {
        return err
    }
    if err := c.Store.Append(record); err != nil {
        return err
    }
    c.Operation = next
    return nil
}

func (c *Coordinator) Abort(at time.Time) {
    c.Cancel()
    c.mu.Lock()
    c.Operation = c.Operation.Abort(at)
    c.mu.Unlock()
}

func (c *Coordinator) Snapshot() model.Operation {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.Operation
}

func (c *Coordinator) ResetContext() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.Context, c.Cancel = context.WithCancel(context.Background())
}
