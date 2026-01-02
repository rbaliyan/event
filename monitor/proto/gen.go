// Package monitorpb provides protobuf definitions for the monitor service.
package monitorpb

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative monitor.proto
