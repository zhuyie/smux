package smux

import "net"

type listener struct {
	sess *Session
}

// NewListener returns a new Listener, accepting streams which arrive from sess.
func NewListener(sess *Session) net.Listener {
	return &listener{
		sess: sess,
	}
}

func (l *listener) Accept() (net.Conn, error) {
	return l.sess.AcceptStream()
}

func (l *listener) Close() error {
	return l.sess.Close()
}

func (l *listener) Addr() net.Addr {
	return &smuxAddr{}
}

type smuxAddr struct{}

func (a *smuxAddr) Network() string { return "smux" }
func (a *smuxAddr) String() string  { return "" }
