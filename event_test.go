package event

import (
	"context"
	"testing"
)

func TestEvent(t *testing.T) {
	e := New("test")
	done := false
	if err := Register(e); err == nil {
		t.Errorf("Failed to register event: %v", err)
	}
	ch := make(chan struct{})
	e.Subscribe(context.TODO(), func(context.Context, Event, Data) {
		ch <- struct{}{}
		done = true
	})
	e.Publish(context.TODO(), nil)
	<-ch
	if !done {
		t.Error("Failed")
	}
	if e.Name() != "test/test" {
		t.Error("Fullname mismatch")
	}
	e1 := Get("test")
	if e1 == nil {
		t.Fatal("Failed to get event")
	}
	done = false
	e1.Publish(context.TODO(), nil)
	<-ch
	if !done {
		t.Error("Failed")
	}
}
