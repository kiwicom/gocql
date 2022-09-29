package gocql

import "context"

// ConcurrencyLimiter allows limiting the number of requests currently in-flight from a session.
type ConcurrencyLimiter interface {
	// Wait until we can write a query to the host.
	// Any error returned from Wait will cause the query to not be written to the network.
	// The error will be returned to the client.
	Wait(ctx context.Context, host *HostInfo) error

	// Done is called when a query/stream is completed.
	Done()
}
