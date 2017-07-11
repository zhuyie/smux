package smux

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	defaultAcceptBacklog = 1024
	defaultWriteQueueLen = 64
	maxFreeWriteReqs     = 1024
	maxBatchWriteLen     = 16
	maxBatchWriteBytes   = 130 * 1024
)

const (
	errBrokenPipe      = "broken pipe"
	errInvalidProtocol = "invalid protocol version"
	errGoAway          = "stream id overflows, should start a new connection"
)

type writeRequest struct {
	frames []Frame
	result chan error
}

func (wr *writeRequest) Len() (n int) {
	for i := 0; i < len(wr.frames); i++ {
		n += headerSize
		n += len(wr.frames[i].data)
	}
	return
}

// Session defines a multiplexed connection for streams
type Session struct {
	conn io.ReadWriteCloser

	config           *Config
	nextStreamID     uint32 // next stream identifier
	nextStreamIDLock sync.Mutex

	bucket       int32         // token bucket
	bucketNotify chan struct{} // used for waiting for tokens

	streams    map[uint32]*Stream // all streams in this session
	streamLock sync.Mutex         // locks streams

	die       chan struct{} // flag session has died
	dieLock   sync.Mutex
	chAccepts chan *Stream

	dataReady int32 // flag data has arrived

	goAway int32 // flag id exhausted

	deadline atomic.Value

	writes        chan *writeRequest
	freeWriteReqs chan *writeRequest
}

func newSession(config *Config, conn io.ReadWriteCloser, client bool) *Session {
	s := new(Session)
	s.die = make(chan struct{})
	s.conn = conn
	s.config = config
	s.streams = make(map[uint32]*Stream)
	s.chAccepts = make(chan *Stream, defaultAcceptBacklog)
	s.bucket = int32(config.MaxReceiveBuffer)
	s.bucketNotify = make(chan struct{}, 1)
	s.writes = make(chan *writeRequest, defaultWriteQueueLen)
	s.freeWriteReqs = make(chan *writeRequest, maxFreeWriteReqs)

	if client {
		s.nextStreamID = 1
	} else {
		s.nextStreamID = 0
	}
	go s.recvLoop()
	go s.sendLoop()
	go s.keepalive()
	return s
}

// OpenStream is used to create a new stream
func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, errors.New(errBrokenPipe)
	}

	// generate stream id
	s.nextStreamIDLock.Lock()
	if s.goAway > 0 {
		s.nextStreamIDLock.Unlock()
		return nil, errors.New(errGoAway)
	}

	s.nextStreamID += 2
	sid := s.nextStreamID
	if sid == sid%2 { // stream-id overflows
		s.goAway = 1
		s.nextStreamIDLock.Unlock()
		return nil, errors.New(errGoAway)
	}
	s.nextStreamIDLock.Unlock()

	stream := newStream(sid, s.config.MaxFrameSize, s)

	if _, err := s.writeFrame(newFrame(cmdSYN, sid, nil)); err != nil {
		return nil, errors.Wrap(err, "writeFrame")
	}

	s.streamLock.Lock()
	s.streams[sid] = stream
	s.streamLock.Unlock()
	return stream, nil
}

// AcceptStream is used to block until the next available stream
// is ready to be accepted.
func (s *Session) AcceptStream() (*Stream, error) {
	var deadline <-chan time.Time
	if d, ok := s.deadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(d.Sub(time.Now()))
		defer timer.Stop()
		deadline = timer.C
	}
	select {
	case stream := <-s.chAccepts:
		return stream, nil
	case <-deadline:
		return nil, errTimeout
	case <-s.die:
		return nil, errors.New(errBrokenPipe)
	}
}

// Close is used to close the session and all streams.
func (s *Session) Close() (err error) {
	s.dieLock.Lock()

	select {
	case <-s.die:
		s.dieLock.Unlock()
		return errors.New(errBrokenPipe)
	default:
		close(s.die)
		s.dieLock.Unlock()
		s.streamLock.Lock()
		for k := range s.streams {
			s.streams[k].sessionClose()
		}
		s.streamLock.Unlock()
		s.notifyBucket()
		return s.conn.Close()
	}
}

// notifyBucket notifies recvLoop that bucket is available
func (s *Session) notifyBucket() {
	select {
	case s.bucketNotify <- struct{}{}:
	default:
	}
}

// IsClosed does a safe check to see if we have shutdown
func (s *Session) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// NumStreams returns the number of currently open streams
func (s *Session) NumStreams() int {
	if s.IsClosed() {
		return 0
	}
	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	return len(s.streams)
}

// SetDeadline sets a deadline used by Accept* calls.
// A zero time value disables the deadline.
func (s *Session) SetDeadline(t time.Time) error {
	s.deadline.Store(t)
	return nil
}

// notify the session that a stream has closed
func (s *Session) streamClosed(sid uint32) {
	s.streamLock.Lock()
	if n := s.streams[sid].recycleTokens(); n > 0 { // return remaining tokens to the bucket
		if atomic.AddInt32(&s.bucket, int32(n)) > 0 {
			s.notifyBucket()
		}
	}
	delete(s.streams, sid)
	s.streamLock.Unlock()
}

// returnTokens is called by stream to return token after read
func (s *Session) returnTokens(n int) {
	if atomic.AddInt32(&s.bucket, int32(n)) > 0 {
		s.notifyBucket()
	}
}

// decodeFrame try to decode a frame from dataBuf
func (s *Session) decodeFrame(dataBuf *[]byte) (f Frame, ok bool, err error) {
	if len(*dataBuf) >= headerSize {
		dec := rawHeader(*dataBuf)
		if dec.Version() != version {
			return f, false, errors.New(errInvalidProtocol)
		}
		if length := int(dec.Length()); len(*dataBuf) >= headerSize+length {
			copy(f.header[:], *dataBuf)
			f.data = (*dataBuf)[headerSize : headerSize+length]
			*dataBuf = (*dataBuf)[headerSize+length:]
			return f, true, nil
		}
	}
	return f, false, nil
}

// recvLoop keeps on reading from underlying connection if tokens are available
func (s *Session) recvLoop() {
	buffer := make([]byte, 130*1024)
	off := 0
	for {
		for atomic.LoadInt32(&s.bucket) <= 0 && !s.IsClosed() {
			<-s.bucketNotify
		}

		n, _ := s.conn.Read(buffer[off:])
		if n <= 0 {
			s.Close()
			return
		}
		off += n

		atomic.StoreInt32(&s.dataReady, 1)

		dataBuf := buffer[:off]
		dataLen := len(dataBuf)
		for {
			f, ok, err := s.decodeFrame(&dataBuf)
			if err != nil {
				s.Close()
				return
			}
			if !ok {
				break
			}

			h := rawHeader(f.header[:])
			switch h.Cmd() {
			case cmdNOP:
			case cmdSYN:
				s.streamLock.Lock()
				if _, ok := s.streams[h.StreamID()]; !ok {
					stream := newStream(h.StreamID(), s.config.MaxFrameSize, s)
					s.streams[h.StreamID()] = stream
					select {
					case s.chAccepts <- stream:
					case <-s.die:
					}
				}
				s.streamLock.Unlock()
			case cmdFIN:
				s.streamLock.Lock()
				if stream, ok := s.streams[h.StreamID()]; ok {
					stream.markRST()
					stream.notifyReadEvent()
				}
				s.streamLock.Unlock()
			case cmdPSH:
				s.streamLock.Lock()
				if stream, ok := s.streams[h.StreamID()]; ok {
					atomic.AddInt32(&s.bucket, -int32(len(f.data)))
					stream.pushBytes(f.data)
					stream.notifyReadEvent()
				}
				s.streamLock.Unlock()
			default:
				s.Close()
				return
			}
		}

		remains := len(dataBuf)
		if remains > 0 {
			if remains != dataLen {
				copy(buffer, dataBuf)
				off = remains
			}
		} else {
			off = 0
		}
	}
}

func (s *Session) keepalive() {
	tickerPing := time.NewTicker(s.config.KeepAliveInterval)
	tickerTimeout := time.NewTicker(s.config.KeepAliveTimeout)
	defer tickerPing.Stop()
	defer tickerTimeout.Stop()
	for {
		select {
		case <-tickerPing.C:
			s.writeFrame(newFrame(cmdNOP, 0, nil))
			s.notifyBucket() // force a signal to the recvLoop
		case <-tickerTimeout.C:
			if !atomic.CompareAndSwapInt32(&s.dataReady, 1, 0) {
				s.Close()
				return
			}
		case <-s.die:
			return
		}
	}
}

func (s *Session) sendLoop() {
	var requests []*writeRequest
	var buffers, tmpBuffers net.Buffers
	for {
		var requestsBytes int
		select {
		case <-s.die:
			return
		case req := <-s.writes:
			requests = append(requests, req)
			requestsBytes += req.Len()
			for q := false; !q; {
				select {
				case <-s.die:
					return
				case req := <-s.writes:
					requests = append(requests, req)
					requestsBytes += req.Len()
					if requestsBytes >= maxBatchWriteBytes || len(requests) >= maxBatchWriteLen {
						q = true
					}
				default:
					q = true
				}
			}
		}

		for _, req := range requests {
			for i := 0; i < len(req.frames); i++ {
				buffers = append(buffers, req.frames[i].header[:])
				if length := len(req.frames[i].data); length > 0 {
					buffers = append(buffers, req.frames[i].data)
				}
			}
		}

		tmpBuffers = buffers
		_, err := tmpBuffers.WriteTo(s.conn)

		for _, req := range requests {
			req.result <- err
		}

		requests = requests[0:0]
		buffers = buffers[0:0]
	}
}

// writeFrame writes the frame to the underlying connection
// and returns the number of bytes written if successful
func (s *Session) writeFrame(f Frame) (n int, err error) {
	req := s.allocWriteRequest()
	req.frames = append(req.frames, f)

	select {
	case s.writes <- req:
	case <-s.die:
		s.freeWriteRequest(req)
		return 0, errors.New(errBrokenPipe)
	}

	select {
	case err = <-req.result:
		s.freeWriteRequest(req)
		return len(f.data), err
	case <-s.die:
		return 0, errors.New(errBrokenPipe)
	}
}

func (s *Session) allocWriteRequest() (req *writeRequest) {
	select {
	case req = <-s.freeWriteReqs:
	default:
	}
	if req == nil {
		req = &writeRequest{}
		req.frames = make([]Frame, 0, 32)
		req.result = make(chan error, 1)
	}
	return
}

func (s *Session) freeWriteRequest(req *writeRequest) {
	req.frames = req.frames[0:0]
	select {
	case s.freeWriteReqs <- req:
	default:
	}
}
