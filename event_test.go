package event

import (
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"
	"math"
	"sync/atomic"
	"syreclabs.com/go/faker"
	"testing"
	"time"
)

func init() {
	faker.Seed(time.Now().UnixNano())
}

const waitChTimeoutMS = 100

func waitForMetaData(ch chan Metadata, timeout int) (Metadata, bool) {
	select {
	case d := <-ch:
		return d, true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return nil, false
	}
}

func waitForData(ch chan Data, timeout int) (Data, bool) {
	select {
	case d := <-ch:
		return d, true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return nil, false
	}
}

func wait(ch chan struct{}, timeout int) bool {
	select {
	case <-ch:
		return true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return false
	}
}

// Compare metadata
func CompareMetadata(m1, m2 Metadata) bool {
	if len(m1) == len(m2) {
		for i, x := range m1 {
			if m2[i] != x {
				return false
			}
		}
		return true
	}
	return false
}

func TestEvent(t *testing.T) {
	// With default registry use cancellable context as they will reuse same registry and event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := defaultRegistry
	e := New("test")
	if err := Register(e); err == nil || !errors.Is(err, ErrDuplicateEvent) {
		t.Errorf("duplicate event registered event: %v", err)
	}
	ch := make(chan struct{})
	e.Subscribe(ctx, func(ctx context.Context, ev Event, data Data) {
		if id := ContextEventID(ctx); id == "" {
			t.Error("event id is null")
		}
		if r1 := ContextRegistry(ctx); r1 == nil {
			t.Error("registry id is null")
		} else if r1.id != r.id {
			t.Errorf("registry is wrong got:%s, expected:%s", r1.id, r.id)
		}
		if source := ContextSource(ctx); source != r.id {
			t.Errorf("source is wrong got:%s, expected:%s", source, r.id)
		}
		if data != nil {
			t.Error("data is not null")
		}
		ch <- struct{}{}
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("Failed")
	}
	e1 := Get("test")
	if e1 == nil {
		t.Fatal("Failed to get event")
	}
	e1.Publish(context.TODO(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("Failed")
	}
}

func TestMetadata(t *testing.T) {
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithMetrics(true, nil),
		WithRegistry(r))

	if r.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}
	ch1 := make(chan Metadata)
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event, _ Data) {
		if m := ContextMetadata(ctx); m == nil {
			t.Fatal("metadata is null")
		} else {
			ch1 <- m
		}
	})
	ch2 := make(chan Metadata)
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event, _ Data) {
		if m := ContextMetadata(ctx); m == nil {
			t.Fatal("metadata is null")
		} else {
			ch2 <- m
		}
	})
	msg := "this is a test"
	m := Metadata([]byte(msg))
	e.Publish(ContextWithMetadata(context.Background(), m), nil)
	m1, ok := waitForMetaData(ch1, waitChTimeoutMS)
	if !ok {
		t.Fatal("metadata not found")
	}
	if !CompareMetadata(m, m1) {
		t.Errorf("metadata is different got:%v, expected:%v", m1, m)
	}
	e.Publish(ContextWithMetadata(context.Background(), m), nil)
	m2, ok := waitForMetaData(ch2, waitChTimeoutMS)
	if !ok {
		t.Fatal("metadata not found")
	}
	if !CompareMetadata(m, m2) {
		t.Errorf("metadata is different got:%v, expected:%v", m2, m)
	}
	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestPanic(t *testing.T) {
	ch1 := make(chan struct{})
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithRecovery(true),
		WithMetrics(true, nil),
		WithRegistry(r),
		WithErrorHandler(func(event Event, err error) {
			ch1 <- struct{}{}
		}))

	if r.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}

	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event, _ Data) {
		panic("test")
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch1, waitChTimeoutMS) {
		t.Error("Panic failed")
	}
	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestCancel(t *testing.T) {
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithMetrics(true, nil), WithRegistry(r))
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	ctx1, cancel1 := context.WithCancel(context.Background())
	e.Subscribe(ctx1, func(ctx context.Context, ev Event, data Data) {
		ch1 <- struct{}{}
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	e.Subscribe(ctx2, func(context.Context, Event, Data) {
		ch2 <- struct{}{}
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if !wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	cancel1()
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if !wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	cancel2()
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	no := faker.RandomInt(0, math.MaxInt-1)
	s := faker.Lorem().String()
	st := struct {
		N int
		S string
	}{no, s}

	tests := []struct {
		name string
		args interface{}
	}{
		{"null", nil},
		{"number", no},
		{"string", s},
		{"struct", st},
	}
	ch := make(chan Data)
	ch1 := make(chan Data)
	ch2 := make(chan Data)
	e := New("test")
	e.Subscribe(ctx, func(ctx context.Context, event Event, data Data) {
		ch <- data
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event, data Data) {
		ch1 <- data
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event, data Data) {
		ch2 <- data
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e.Publish(context.Background(), tt.args)
			out, ok := waitForData(ch, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out, tt.args))
			}
			out1, ok := waitForData(ch1, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out1, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out1, tt.args))
			}
			out2, ok := waitForData(ch2, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out2, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out2, tt.args))
			}
		})
	}
}

func BenchmarkEvent(b *testing.B) {
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithMetrics(true, nil), WithRegistry(r))
	ch1 := make(chan struct{})
	total := int32(b.N)
	var counter int32
	e.Subscribe(context.Background(), func(ctx context.Context, event Event, data Data) {
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	for i := 0; i < b.N; i++ {
		e.Publish(context.Background(), i)
	}
	e.Publish(context.Background(), -1)
	if !wait(ch1, 2000) {
		b.Error("pubTimeout")
	}
	if counter < int32(b.N) {
		b.Error("counter is smaller :", counter, b.N)
	}
	if err := r.Close(); err != nil {
		b.Error("failed to close registry")
	}
}

func TestPool(t *testing.T) {
	var poolSize int32 = 4
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithWorkerPoolSize(uint(poolSize)),
		WithMetrics(true, nil), WithRegistry(r))
	var total int32 = 100
	var counter int32
	var counter1 int32
	var max int32
	ch := make(chan int32)
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	go func() {
		for {
			select {
			case count := <-ch:
				if count > max {
					max = count
				}
			case <-ch1:
				if atomic.AddInt32(&counter1, 1) >= total {
					ch2 <- struct{}{}
				}
			}
		}
	}()
	e.Subscribe(context.Background(), func(ctx context.Context, event Event, data Data) {
		defer atomic.AddInt32(&counter, -1)
		ch <- atomic.AddInt32(&counter, 1)
		ch1 <- struct{}{}
	})
	var i int32
	for i = 0; i < total; i++ {
		e.Publish(context.Background(), i)
	}
	if !wait(ch2, 2000) {
		t.Error("pubTimeout")
	}
	if max > total/2 {
		t.Error("Failed")
	}
}

func TestSingleTransport(t *testing.T) {
	var poolSize int32 = 4
	r := NewRegistry("test", nil)
	e := New("test",
		WithAsync(true),
		WithTracing(true),
		WithPublishTimeout(time.Duration(100)*time.Millisecond),
		WithTransport(NewSingleTransport(time.Duration(1)*time.Second, 100)),
		WithWorkerPoolSize(uint(poolSize)),
		WithMetrics(true, nil), WithRegistry(r))
	var total int32 = 100
	var counter int32
	var counter1 int32
	var counter2 int32
	var counter3 int32
	ch1 := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event, data Data) {
		atomic.AddInt32(&counter1, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event, data Data) {
		atomic.AddInt32(&counter2, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event, data Data) {
		atomic.AddInt32(&counter3, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	var i int32
	for i = 0; i < total; i++ {
		e.Publish(context.Background(), i)
	}
	if !wait(ch1, 2000) {
		t.Error("pubTimeout", counter1)
	}
	if counter != total {
		t.Error("Failed", counter, total)
	}
	if counter1 >= total {
		t.Error("Failed", counter1, total)
	}
	if counter2 >= total {
		t.Error("Failed", counter2, total)
	}
	if counter3 >= total {
		t.Error("Failed", counter3, total)
	}
}
