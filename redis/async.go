package redis

import (
	"time"
)

type tResult struct {
	result interface{}
	err    error
}

type tRequest struct {
	cmd  string
	args []interface{}
	c    chan *tResult
}

// send to doreply
type tReply struct {
	cmd string
	c   chan *tResult
}

// result interface
type Ret interface {
	Get() (reply interface{}, err error)
}

// sync result
type TRet struct {
	Reply interface{}
	Err    error
}

// async result
type ARet struct {
	c chan *tResult
}

// Do command to redis server,the goroutine of caller is not suspended.
func (c *conn) AsyncDo(cmd string, args ...interface{}) (Ret, error) {
	if cmd == "" {
		return nil, nil
	}

	retChan := make(chan *tResult, 2)

	c.reqChan <- &tRequest{cmd: cmd, args: args, c: retChan}

	return &ARet{c: retChan}, nil
}

func (c *conn) doRequest() {
	for {
		select {
		case <-c.closeReqChan:
			close(c.reqChan)
			c.closeRepChan <- true
			return

		case req := <-c.reqChan:
			for i, length := 0, len(c.reqChan); ; {
				if c.writeTimeout != 0 {
					c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
				}
				if err := c.writeCommand(req.cmd, req.args); err != nil {
					req.c <- &tResult{nil, err}
					c.fatal(err)
					break
				}
				req.c <- &tResult{nil, nil}
				c.repChan <- &tReply{cmd: req.cmd, c: req.c}
				if i++; i > length {
					break
				}
				req = <-c.reqChan
			}
		}

		if err := c.bw.Flush(); err != nil {
			c.fatal(err)
			continue
		}
	}
}

func (c *conn) doReply() {
	for {
		select {
		case <-c.closeRepChan:
			close(c.repChan)
			return

		case rep := <-c.repChan:
			if c.readTimeout != 0 {
				c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			}
			reply, err := c.readReply()
			if err != nil {
				rep.c <- &tResult{nil, err}
				c.fatal(err)
				continue
			} else {
				c.t = nowFunc()
			}
			if e, ok := reply.(Error); ok {
				err = e
			}
			rep.c <- &tResult{reply, err}
		}
	}
}

// get command result received
func (t *TRet) Get() (interface{}, error) {
	return t.Reply,t.Err
}

// get command result received async
func (a *ARet) Get() (interface{}, error) {
	send := <-a.c
	if send.err != nil {
		return send.result, send.err
	}
	recv := <-a.c
	return recv.result, recv.err
}