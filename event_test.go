package event

import (
	"context"
	"testing"
	"time"
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

func TestCancel(t *testing.T) {
	e := New("test")

	if err := Register(e); err == nil {
		t.Errorf("Failed to register event: %v", err)
	}
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	wait := func(ch chan struct{}) bool {
		select {
		case <-ch:
			return true
		case <-time.After(time.Second):
			return false
		}
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	e.Subscribe(ctx1, func(context.Context, Event, Data) {
		ch1 <- struct{}{}
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	e.Subscribe(ctx2, func(context.Context, Event, Data) {
		ch2 <- struct{}{}
	})
	e.Publish(context.TODO(), nil)

	if !wait(ch1) {
		t.Error("1. Failed")
	}
	if !wait(ch2) {
		t.Error("2. Failed")
	}
	cancel1()
	e.Publish(context.TODO(), nil)
	if wait(ch1) {
		t.Error("1. Failed")
	}
	if !wait(ch2) {
		t.Error("2. Failed")
	}
	cancel2()
	e.Publish(context.TODO(), nil)
	if wait(ch1) {
		t.Error("1. Failed")
	}
	if wait(ch2) {
		t.Error("2. Failed")
	}
}
