// Package event provides mechanism for publishing and subscribing event using abstract transport.
// Default available transport is a channel map with fan-out strategy.
//
// To handle remote events other transports such as Redis or Nats can also be used.
//
// example:
//
//  e := New("event name")
//  e.Subscribe(context.Background(), func(ctx context.Context, ev Event, data Data){
//      fmt.Println("msg>", data)
//  })
//
//  e.Publish(context.Background(), "success")
//
//
// Available Options:
// WithPublishTimeout set timeout in milliseconds for event publishing. Default is 1 second.
// if set to 0, timeout will be disabled and publisher will wait indefinitely.
// WithAsyncTimeout set async timeout in milliseconds for event in async mode.  Default is 5 second.
// if set to 0, timeout will be disabled and handlers will wait indefinitely.
// WithSubscriberTimeout set subscriber timeout in milliseconds for event subscribers. Default is 30 second.
// if set to 0, timeout will be disabled and handlers will  wait indefinitely.
// WithTracing enable/disable tracing for event. Default is true.
// WithAsync enable/disable async handlers for event. Default is true.
// if async handlers are disabled, event handlers are run in
// one single go routine and timeout value from WithPublishTimeout is applied
// on publishing time which might cause server to drop events.
// when async mode is enabled the order of events is not guaranteed.
// WithMetrics  enable/disable prometheus metrics for event. Default is true.
// WithErrorHandler set error handler for event.
// WithTransport set transport for event. Default is Channel.
// WithLogger set logger for event.
// WithWorkerPoolSize set worker pool size. Default is 100.
// This value decides number of subscribers that can execute in parallel.
// WithRegistry set registry for event, if not defaultRegistry is used.
//
//
// Registry defines the scope of events i.e. in one registry there can be only event
// with given name. Optionally registry also holds the information for prometheus.Registerer
// When Registry.Close function is called all events registered in the registry will
// stop publishing data.
//
// Transport defines the transport layer used by events. default is a channels based transport.
// It can be shared among events to make event aliases or sending data across event Registry.
//
//
package event
