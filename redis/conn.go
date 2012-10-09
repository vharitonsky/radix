package redis

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"regexp"
	"strconv"
	"time"
)

//* Misc

var infoRe *regexp.Regexp = regexp.MustCompile(`(?:(\w+):(\w+))+`)

type call struct {
	cmd  Cmd
	args []interface{}
}

//* Conn

// Conn a Redis client.
// Multiple goroutines should not invoke methods on a Conn simultaneously. 
type Conn struct {
	conn          net.Conn
	reader        *bufio.Reader
	config        Config
	submode bool // are we in subscription mode?
	subs map[string]struct{} // set of subscriptions
	psubs map[string]struct{} // set of pattern subscriptions
}

// NewConn creates a new Conn.
func NewConn(config Config)  (*Conn, *Error) {
	var err error
	c := new(Conn)
	c.config = config
	c.subs = make(map[string]struct{})
	c.psubs = make(map[string]struct{})

	// Establish a connection.
	c.conn, err = net.Dial(c.config.Network, c.config.Address)
	if err != nil {
		c.Close()
		return nil, newError(err.Error(), ErrorConnection)
	}
	c.reader = bufio.NewReaderSize(c.conn, 4096)

	// Authenticate if needed.
	if c.config.Password != "" {
		r := c.call(cmdAuth, c.config.Password)
		if r.Err != nil {
			c.Close()
			return nil, newErrorExt("authentication failed", r.Err, ErrorAuth)
		}
	}

	// Select database.
	r := c.call(cmdSelect, c.config.Database)
	if r.Err != nil {
		if c.config.RetryLoading && r.Err.Test(ErrorLoading) {
			// Attempt to read remaining loading time with INFO and sleep that time.
			info, err := c.InfoMap()
			if err == nil {
				if _, ok := info["loading_eta_seconds"]; ok {
					eta, err := strconv.Atoi(info["loading_eta_seconds"])
					if err == nil {
						time.Sleep(time.Duration(eta) * time.Second)
					}
				}
			}

			// Keep retrying SELECT until it succeeds or we got some other error.
			r = c.call(cmdSelect, c.config.Database)
			for r.Err != nil {
				if !r.Err.Test(ErrorLoading) {
					goto SelectFail
				}
				time.Sleep(time.Second)
				r = c.call(cmdSelect, c.config.Database)
			}
		}

	SelectFail:
		c.Close()
		return nil, newErrorExt("selecting database failed", r.Err)
	}

	return c, nil
}

// Close closes the connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Call calls the given Redis command.
func (c *Conn) Call(cmd string, args ...interface{}) *Reply {
	return c.call(Cmd(cmd), args...)
}

// AsyncCall calls the given Redis command asynchronously.
func (c *Conn) AsyncCall(cmd string, args ...interface{}) Future {
	return c.asyncCall(Cmd(cmd), args...)
}

// MultiCall executes the given MultiCall.
// Multicall reply is guaranteed to have the same number of sub-replies as calls, if it succeeds.
func (c *Conn) MultiCall(f func(*MultiCall)) *Reply {
	return newMultiCall(false, c).process(f)
}

// Transaction performs a simple transaction.
// Simple transaction is a multi command that is wrapped in a MULTI-EXEC block.
// For complex transactions with WATCH, UNWATCH or DISCARD commands use MultiCall.
// Transaction reply is guaranteed to have the same number of sub-replies as calls, if it succeeds.
func (c *Conn) Transaction(f func(*MultiCall)) *Reply {
	return newMultiCall(true, c).process(f)
}

// AsyncMultiCall calls an asynchronous MultiCall.
func (c *Conn) AsyncMultiCall(mc func(*MultiCall)) Future {
	f := newFuture()
	go func() {
		f <- c.MultiCall(mc)
	}()
	return f
}

// AsyncTransaction performs a simple asynchronous transaction.
func (c *Conn) AsyncTransaction(mc func(*MultiCall)) Future {
	f := newFuture()
	go func() {
		f <- c.Transaction(mc)
	}()
	return f
}

// InfoMap calls the INFO command, parses and returns the results as a map[string]string or an error. 
// Use Info() method for fetching the unparsed INFO results.
func (c *Conn) InfoMap() (map[string]string, error) {
	info := make(map[string]string)
	r := c.call(cmdInfo)
	s, err := r.Str()
	if err != nil {
		return nil, err
	}
	for _, e := range infoRe.FindAllStringSubmatch(s, -1) {
		if len(e) != 3 {
			return nil, errors.New("failed to parse INFO results")
		}
		info[e[1]] = e[2]
	}
	return info, nil
}

/*
// Subscription returns a new Subscription instance with the given message handler callback or
// an error. The message handler is called whenever a new message arrives.
// Subscriptions create their own dedicated connections,
// they do not pull connections from the connection pool.
func (c *Conn) Subscription(msgHdlr func(msg *Message)) (*Subscription, *Error) {
	if msgHdlr == nil {
		panic(errmsg("message handler must not be nil"))
	}

	sub, err := newSubscription(&c.config, msgHdlr)
	if err != nil {
		return nil, err
	}

	return sub, nil
}
*/

//* Private methods

func (c *Conn) call(cmd Cmd, args ...interface{}) *Reply {
	if c.submode {
		return &Reply{Type: ReplyError, Err: err}
	if err := c.writeRequest(call{cmd, args}); err != nil {
		return &Reply{Type: ReplyError, Err: err}
	} 
	return c.read()
}

func (c *Conn) asyncCall(cmd Cmd, args ...interface{}) Future {
	f := newFuture()
	go func() {
		f <- c.call(cmd, args...)
	}()
	return f
}

// multiCall calls multiple Redis commands.
func (c *Conn) multiCall(cmds []call) (r *Reply) {
	r = new(Reply)
	if err := c.writeRequest(cmds...); err == nil {
		r.Type = ReplyMulti
		r.Elems = make([]*Reply, len(cmds))
		for i := range cmds {
			reply := c.read()
			r.Elems[i] = reply
		}
	} else {
		r.Err = newError(err.Error())
	}

	return r
}

// subscription handles subscribe, unsubscribe, psubscribe and pubsubscribe calls.
func (c *Conn) subscription(subType subType, data []string) *Error {
	// Prepare command.
	var cmd Cmd

	switch subType {
	case subSubscribe:
		cmd = cmdSubscribe
	case subUnsubscribe:
		cmd = cmdUnsubscribe
	case subPsubscribe:
		cmd = cmdPsubscribe
	case subPunsubscribe:
		cmd = cmdPunsubscribe
	}

	// Send the subscription request.
	channels := make([]interface{}, len(data))
	for i, v := range data {
		channels[i] = v
	}

	err := c.writeRequest(call{cmd, channels})
	if err == nil {
		return nil
	}

	return newError(err.Error())
	// subscribe/etc. return their replies as pubsub messages
}

// helper for read()
func (c *Conn) readErrHdlr(err error) (r *Reply) {
	if err != nil {
		c.Close()
		err_, ok := err.(net.Error)
		if ok && err_.Timeout() {
			return &Reply{
				Type: ReplyError,
				Err: newError("read failed, timeout error: "+err.Error(), ErrorConnection,
					ErrorTimeout),
			}
		}

		return &Reply{
			Type: ReplyError,
			Err:  newError("read failed: "+err.Error(), ErrorConnection),
		}
	}

	return nil
}

// read reads data from the connection and returns a Reply.
func (c *Conn) read() (r *Reply) {
	var err error
	var b []byte
	r = new(Reply)

	if !c.submode {
		c.setReadTimeout()
	}
	b, err = c.reader.ReadBytes('\n')
	if re := c.readErrHdlr(err); re != nil {
		return re
	}

	// Analyze the first byte.
	fb := b[0]
	b = b[1 : len(b)-2] // get rid of the first byte and the trailing \r
	switch fb {
	case '-':
		// Error reply.
		r.Type = ReplyError
		switch {
		case bytes.HasPrefix(b, []byte("ERR")):
			r.Err = newError(string(b[4:]), ErrorRedis)
		case bytes.HasPrefix(b, []byte("LOADING")):
			r.Err = newError("Redis is loading data into memory", ErrorRedis, ErrorLoading)
		default:
			// this shouldn't really ever execute
			r.Err = newError(string(b), ErrorRedis)
		}
	case '+':
		// Status reply.
		r.Type = ReplyStatus
		r.str = string(b)
	case ':':
		// Integer reply.
		var i int64
		i, err = strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			r.Type = ReplyError
			r.Err = newError("integer reply parse error", ErrorParse)
		} else {
			r.Type = ReplyInteger
			r.int = i
		}
	case '$':
		// Bulk reply, or key not found.
		var i int
		i, err = strconv.Atoi(string(b))
		if err != nil {
			r.Type = ReplyError
			r.Err = newError("bulk reply parse error", ErrorParse)
		} else {
			if i == -1 {
				// Key not found
				r.Type = ReplyNil
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				rc := 0

				for rc < ir {
					if !c.submode {
						c.setReadTimeout()
					}
					n, err := c.reader.Read(br[rc:])
					if re := c.readErrHdlr(err); re != nil {
						return re
					}

					rc += n
				}
				s := string(br[0:i])
				r.Type = ReplyString
				r.str = s
			}
		}
	case '*':
		// Multi-bulk reply. Just return the count
		// of the replies. The caller has to do the
		// individual calls.
		var i int
		i, err = strconv.Atoi(string(b))
		if err != nil {
			r.Type = ReplyError
			r.Err = newError("multi-bulk reply parse error", ErrorParse)
		} else {
			switch {
			case i == -1:
				// nil multi-bulk
				r.Type = ReplyNil
			case i >= 0:
				r.Type = ReplyMulti
				r.Elems = make([]*Reply, i)
				for i := range r.Elems {
					r.Elems[i] = c.read()
				}
			default:
				// invalid reply
				r.Type = ReplyError
				r.Err = newError("received invalid reply", ErrorParse)
			}
		}
	default:
		// invalid reply
		r.Type = ReplyError
		r.Err = newError("received invalid reply", ErrorParse)
	}

	return r
}

func (c *Conn) writeRequest(calls ...call) *Error {
	c.setWriteTimeout()
	if _, err := c.conn.Write(createRequest(calls...)); err != nil {
		errn, ok := err.(net.Error)
		if ok && errn.Timeout() {
			return newError("write failed, timeout error: "+err.Error(),
				ErrorConnection, ErrorTimeout)
		}
		return newError("write failed: "+err.Error(), ErrorConnection)
	}
	return nil
}

func (c *Conn) setReadTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.Timeout))
	}
}

func (c *Conn) setWriteTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.Timeout))
	}
}
