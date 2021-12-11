package tcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/muirglacier/aw/policy"
)

// Listen for connections from remote peers until the context is done. The
// allow function will be used to control the acceptance/rejection of connection
// attempts, and can be used to implement maximum connection limits, per-IP
// rate-limiting, and so on. This function spawns all accepted connections into
// their own background goroutines that run the handle function, and then
// clean-up the connection. This function blocks until the context is done.
func Listen(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), allow policy.Allow) error {
	// Create a TCP listener from given address and return an error if unable to do so
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}

	// The 'ctx' we passed to Listen() will not unblock `Listener.Accept()` if
	// context exceeding the deadline. We need to manually close the listener
	// to stop `Listener.Accept()` from blocking.
	// See https://github.com/golang/go/issues/28120
	go func() {
		<- ctx.Done()
		listener.Close()
	}()
	return ListenWithListener(ctx, listener, handle, handleErr, allow)
}

// ListenWithListener is the same as Listen but instead of specifying an
// address, it accepts an already constructed listener.
//
// NOTE: The listener passed to this function will be closed when the given
// context finishes.
func ListenWithListener(ctx context.Context, listener net.Listener, handle func(net.Conn), handleErr func(error), allow policy.Allow) error {
	if handle == nil {
		return fmt.Errorf("nil handle function")
	}

	if handleErr == nil {
		handleErr = func(err error) {}
	}

	defer listener.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			handleErr(fmt.Errorf("accept connection: %w", err))
			continue
		}

		if allow == nil {
			go func() {
				defer conn.Close()

				handle(conn)
			}()
			continue
		}

		if err, cleanup := allow(conn); err == nil {
			go func() {
				defer conn.Close()

				defer func() {
					if cleanup != nil {
						cleanup()
					}
				}()
				handle(conn)
			}()
			continue
		}
		conn.Close()
	}
}

// ListenerWithAssignedPort creates a new listener on a random port assigned by
// the OS. On success, both the listener and port are returned.
func ListenerWithAssignedPort(ctx context.Context, ip string) (net.Listener, int, error) {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", fmt.Sprintf("%v:%v", ip, 0))
	if err != nil {
		return nil, 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	return listener, port, nil
}

// Dial a remote peer until a connection is successfully established, or until
// the context is done. Multiple dial attempts can be made, and the timeout
// function is used to define an upper bound on dial attempts. This function
// blocks until the connection is handled (and the handle function returns).
// This function will clean-up the connection.
func Dial(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), timeout func(int) time.Duration) error {
	dialer := new(net.Dialer)

	if handle == nil {
		return fmt.Errorf("nil handle function")
	}

	if handleErr == nil {
		handleErr = func(error) {}
	}

	if timeout == nil {
		timeout = func(int) time.Duration { return time.Second }
	}

	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("dialing %w", ctx.Err())
		default:
		}

		dialCtx, dialCancel := context.WithTimeout(ctx, timeout(attempt))
		conn, err := dialer.DialContext(dialCtx, "tcp", address)
		if err != nil {
			handleErr(err)
			<-dialCtx.Done()
			dialCancel()
			continue
		}
		dialCancel()

		return func() (err error) {
			defer func() {
				err = conn.Close()
			}()

			handle(conn)
			return
		}()
	}
}
