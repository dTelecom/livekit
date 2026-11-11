package turn

import (
	"encoding/binary"
	"errors"
	"net"
	"time"

	"github.com/pion/stun"
	"github.com/pion/turn/v2/internal/proto"
)

var (
	errInvalidTURNFrame    = errors.New("data is not a valid TURN frame, no STUN or ChannelData found")
	errIncompleteTURNFrame = errors.New("data contains incomplete STUN or TURN frame")
	errTURNFrameTooLarge   = errors.New("TURN frame is larger than the supplied buffer")
)

// STUNConn wraps a net.Conn and implements
// net.PacketConn by being STUN aware and
// packetizing the stream
type STUNConn struct {
	nextConn net.Conn
	buff     []byte
}

const (
	stunHeaderSize = 20

	channelDataLengthSize = 2
	channelDataNumberSize = channelDataLengthSize
	channelDataHeaderSize = channelDataLengthSize + channelDataNumberSize
	channelDataPadding    = 4
)

// Given a buffer give the last offset of the TURN frame
// If the buffer isn't a valid STUN or ChannelData packet
// or the length doesn't match return false
func consumeSingleTURNFrame(p []byte) (int, error) {
	// Too short to determine if ChannelData or STUN
	if len(p) < 9 {
		return 0, errIncompleteTURNFrame
	}

	// dTelecom patch (backport of upstream pion/turn master): use uint32, not
	// uint16, for the frame size. A STUN length >= 0xFFEC or a ChannelData
	// length >= 0xFFFC plus its header overflows uint16 and wraps to 0, making
	// ReadFrom report an empty frame with a nil error without consuming any
	// buffered data, so the server read loop spins forever.
	var datagramSize uint32
	switch {
	case stun.IsMessage(p):
		datagramSize = uint32(binary.BigEndian.Uint16(p[2:4])) + stunHeaderSize
	case proto.ChannelNumber(binary.BigEndian.Uint16(p[0:2])).Valid():
		dataLen := uint32(binary.BigEndian.Uint16(p[channelDataNumberSize:channelDataHeaderSize]))
		// Round up to the nearest multiple of the 4-byte padding.
		paddedDataLen := (dataLen + channelDataPadding - 1) &^ (channelDataPadding - 1)
		datagramSize = paddedDataLen + channelDataHeaderSize
	case len(p) < stunHeaderSize:
		return 0, errIncompleteTURNFrame
	default:
		return 0, errInvalidTURNFrame
	}

	if uint64(len(p)) < uint64(datagramSize) {
		return 0, errIncompleteTURNFrame
	}

	return int(datagramSize), nil
}

// ReadFrom implements ReadFrom from net.PacketConn
func (s *STUNConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	// First pass any buffered data from previous reads
	n, err = consumeSingleTURNFrame(s.buff)
	if errors.Is(err, errInvalidTURNFrame) {
		return 0, nil, err
	} else if err == nil {
		// dTelecom patch (backport of upstream pion/turn master): reject a
		// frame whose declared size does not fit in the caller's buffer
		// instead of reporting more bytes than were copied. net.PacketConn
		// forbids n > len(p), and Server.readLoop slices buf[:n], which
		// panicked and crashed the whole process. The frame is consumed so
		// the stream framing stays aligned for the next read.
		if n > len(p) {
			s.buff = s.buff[n:]

			return 0, nil, errTURNFrameTooLarge
		}
		copy(p, s.buff[:n])
		s.buff = s.buff[n:]

		return n, s.nextConn.RemoteAddr(), nil
	}

	// Then read from the nextConn, appending to our buff
	n, err = s.nextConn.Read(p)
	if err != nil {
		return 0, nil, err
	}

	s.buff = append(s.buff, append([]byte{}, p[:n]...)...)
	return s.ReadFrom(p)
}

// WriteTo implements WriteTo from net.PacketConn
func (s *STUNConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return s.nextConn.Write(p)
}

// Close implements Close from net.PacketConn
func (s *STUNConn) Close() error {
	return s.nextConn.Close()
}

// LocalAddr implements LocalAddr from net.PacketConn
func (s *STUNConn) LocalAddr() net.Addr {
	return s.nextConn.LocalAddr()
}

// SetDeadline implements SetDeadline from net.PacketConn
func (s *STUNConn) SetDeadline(t time.Time) error {
	return s.nextConn.SetDeadline(t)
}

// SetReadDeadline implements SetReadDeadline from net.PacketConn
func (s *STUNConn) SetReadDeadline(t time.Time) error {
	return s.nextConn.SetReadDeadline(t)
}

// SetWriteDeadline implements SetWriteDeadline from net.PacketConn
func (s *STUNConn) SetWriteDeadline(t time.Time) error {
	return s.nextConn.SetWriteDeadline(t)
}

// NewSTUNConn creates a STUNConn
func NewSTUNConn(nextConn net.Conn) *STUNConn {
	return &STUNConn{nextConn: nextConn}
}
