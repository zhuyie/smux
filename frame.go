package smux

import (
	"encoding/binary"
	"fmt"
)

const (
	version = 1
)

const ( // cmds
	cmdSYN byte = iota // stream open
	cmdFIN             // stream close, a.k.a EOF mark
	cmdPSH             // data push
	cmdNOP             // no operation
)

const (
	sizeOfVer    = 1
	sizeOfCmd    = 1
	sizeOfLength = 2
	sizeOfSid    = 4
	headerSize   = sizeOfVer + sizeOfCmd + sizeOfSid + sizeOfLength
)

// Frame defines a packet from or to be multiplexed into a single connection
type Frame struct {
	header [headerSize]byte
	data   []byte
}

func newFrame(cmd byte, sid uint32, data []byte) (f Frame) {
	f.header[0] = version
	f.header[1] = cmd
	binary.LittleEndian.PutUint16(f.header[2:], uint16(len(data)))
	binary.LittleEndian.PutUint32(f.header[4:], sid)
	f.data = data
	return
}

type rawHeader []byte

func (h rawHeader) Version() byte {
	return h[0]
}

func (h rawHeader) Cmd() byte {
	return h[1]
}

func (h rawHeader) Length() uint16 {
	return binary.LittleEndian.Uint16(h[2:])
}

func (h rawHeader) StreamID() uint32 {
	return binary.LittleEndian.Uint32(h[4:])
}

func (h rawHeader) String() string {
	return fmt.Sprintf("Version:%d Cmd:%d StreamID:%d Length:%d",
		h.Version(), h.Cmd(), h.StreamID(), h.Length())
}
