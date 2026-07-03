// Package msgpacklimit provides pre-decode validation for msgpack data.
//
// The vmihailenco/msgpack/v5 library has a bug in decode_slice.go where the
// allocation limit check uses != 1 instead of != 0 for the disableAllocLimitFlag
// comparison. Since the flag value is 8 (1 << 3), the condition is always true,
// meaning allocation limits never apply for general slice decoding. Additionally,
// interface{} slice decoding has no limit at all.
//
// This package provides a lightweight scanner that walks the msgpack format
// headers and rejects inputs that declare collection sizes exceeding safe limits.
// It enforces both per-collection and cumulative element budgets to prevent a
// small crafted input from triggering multi-GB memory allocations through nested
// collections. It likewise rejects str/bin/ext blobs whose declared byte length
// exceeds the bytes actually present, which would otherwise drive the same
// pre-allocation OOM (e.g. a 9-byte input declaring a 4GB bin32 payload).
package msgpacklimit

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxCollectionElements is the maximum number of elements allowed in any
// single msgpack array or map. Set conservatively because the msgpack library
// allocates capacity upfront (e.g., make([]interface{}, 0, n)) and ASan-instrumented
// builds amplify this overhead significantly.
const MaxCollectionElements = 100_000

// maxTotalElements is the cumulative budget for all declared collection elements.
// This prevents nested collections (each under MaxCollectionElements) from causing
// excessive total memory allocation through stacked pre-allocations.
const maxTotalElements = 1_000_000

// ErrOversizedCollection is returned when a msgpack input declares a collection
// larger than MaxCollectionElements or exceeds the total element budget.
var ErrOversizedCollection = errors.New("msgpack: collection size exceeds safety limit")

// ErrOversizedBlob is returned when a msgpack str/bin/ext header declares a byte
// length larger than the bytes actually present in the input. A blob occupies
// exactly its declared length immediately after the header, so a length beyond
// the remaining input is malformed -- and decoding it would make the msgpack
// library pre-allocate that many bytes (a multi-GB OOM vector). It is rejected
// before decoding rather than silently clamped.
var ErrOversizedBlob = errors.New("msgpack: blob size exceeds input length")

// scanner tracks cumulative element counts during validation.
type scanner struct {
	data       []byte
	totalElems int
}

// Validate scans msgpack-encoded data and returns an error if any array or map
// header declares more than MaxCollectionElements entries or if the total declared
// elements across all collections exceeds the cumulative budget.
func Validate(data []byte) error {
	s := scanner{data: data}
	_, err := s.scan(0)
	return err
}

// addElements tracks cumulative element declarations and rejects if budget exceeded.
func (s *scanner) addElements(n int) error {
	s.totalElems += n
	if s.totalElems > maxTotalElements {
		return fmt.Errorf("%w: total declared elements (%d) exceed budget", ErrOversizedCollection, s.totalElems)
	}
	return nil
}

// scan walks one msgpack value starting at offset, returning the offset after the value.
func (s *scanner) scan(off int) (int, error) {
	if off >= len(s.data) {
		return off, nil
	}

	c := s.data[off]
	off++

	switch {
	// positive fixint (0x00 - 0x7f)
	case c <= 0x7f:
		return off, nil

	// fixmap (0x80 - 0x8f)
	case c >= 0x80 && c <= 0x8f:
		n := int(c & 0x0f)
		return s.scanMap(off, n)

	// fixarray (0x90 - 0x9f)
	case c >= 0x90 && c <= 0x9f:
		n := int(c & 0x0f)
		return s.scanArray(off, n)

	// fixstr (0xa0 - 0xbf)
	case c >= 0xa0 && c <= 0xbf:
		n := int(c & 0x1f)
		return skipBytes(s.data, off, n)

	// nil, false, true (0xc0, 0xc2, 0xc3)
	case c == 0xc0 || c == 0xc2 || c == 0xc3:
		return off, nil

	// bin 8, bin 16, bin 32 (0xc4 - 0xc6)
	case c == 0xc4:
		return skipSized(s.data, off, 1)
	case c == 0xc5:
		return skipSized(s.data, off, 2)
	case c == 0xc6:
		return skipSized(s.data, off, 4)

	// ext 8, ext 16, ext 32 (0xc7 - 0xc9)
	case c == 0xc7:
		return skipExtSized(s.data, off, 1)
	case c == 0xc8:
		return skipExtSized(s.data, off, 2)
	case c == 0xc9:
		return skipExtSized(s.data, off, 4)

	// float 32, float 64 (0xca, 0xcb)
	case c == 0xca:
		return skipBytes(s.data, off, 4)
	case c == 0xcb:
		return skipBytes(s.data, off, 8)

	// uint 8, uint 16, uint 32, uint 64 (0xcc - 0xcf)
	case c == 0xcc:
		return skipBytes(s.data, off, 1)
	case c == 0xcd:
		return skipBytes(s.data, off, 2)
	case c == 0xce:
		return skipBytes(s.data, off, 4)
	case c == 0xcf:
		return skipBytes(s.data, off, 8)

	// int 8, int 16, int 32, int 64 (0xd0 - 0xd3)
	case c == 0xd0:
		return skipBytes(s.data, off, 1)
	case c == 0xd1:
		return skipBytes(s.data, off, 2)
	case c == 0xd2:
		return skipBytes(s.data, off, 4)
	case c == 0xd3:
		return skipBytes(s.data, off, 8)

	// fixext 1, 2, 4, 8, 16 (0xd4 - 0xd8)
	case c == 0xd4:
		return skipBytes(s.data, off, 2) // type(1) + data(1)
	case c == 0xd5:
		return skipBytes(s.data, off, 3) // type(1) + data(2)
	case c == 0xd6:
		return skipBytes(s.data, off, 5) // type(1) + data(4)
	case c == 0xd7:
		return skipBytes(s.data, off, 9) // type(1) + data(8)
	case c == 0xd8:
		return skipBytes(s.data, off, 17) // type(1) + data(16)

	// str 8, str 16, str 32 (0xd9 - 0xdb)
	case c == 0xd9:
		return skipSized(s.data, off, 1)
	case c == 0xda:
		return skipSized(s.data, off, 2)
	case c == 0xdb:
		return skipSized(s.data, off, 4)

	// array 16 (0xdc)
	case c == 0xdc:
		if off+2 > len(s.data) {
			return off, nil
		}
		n := int(binary.BigEndian.Uint16(s.data[off:]))
		off += 2
		return s.scanArray(off, n)

	// array 32 (0xdd)
	case c == 0xdd:
		if off+4 > len(s.data) {
			return off, nil
		}
		n := int(binary.BigEndian.Uint32(s.data[off:]))
		off += 4
		return s.scanArray(off, n)

	// map 16 (0xde)
	case c == 0xde:
		if off+2 > len(s.data) {
			return off, nil
		}
		n := int(binary.BigEndian.Uint16(s.data[off:]))
		off += 2
		return s.scanMap(off, n)

	// map 32 (0xdf)
	case c == 0xdf:
		if off+4 > len(s.data) {
			return off, nil
		}
		n := int(binary.BigEndian.Uint32(s.data[off:]))
		off += 4
		return s.scanMap(off, n)

	// negative fixint (0xe0 - 0xff)
	case c >= 0xe0:
		return off, nil

	default:
		// Unknown/unused code (0xc1), skip
		return off, nil
	}
}

func (s *scanner) scanArray(off, n int) (int, error) {
	if n > MaxCollectionElements {
		return 0, fmt.Errorf("%w: array declares %d elements", ErrOversizedCollection, n)
	}
	if err := s.addElements(n); err != nil {
		return 0, err
	}
	var err error
	for i := 0; i < n && off < len(s.data); i++ {
		off, err = s.scan(off)
		if err != nil {
			return 0, err
		}
	}
	return off, nil
}

func (s *scanner) scanMap(off, n int) (int, error) {
	if n > MaxCollectionElements {
		return 0, fmt.Errorf("%w: map declares %d elements", ErrOversizedCollection, n)
	}
	if err := s.addElements(n); err != nil {
		return 0, err
	}
	var err error
	for i := 0; i < n && off < len(s.data); i++ {
		// key
		off, err = s.scan(off)
		if err != nil {
			return 0, err
		}
		// value
		if off < len(s.data) {
			off, err = s.scan(off)
			if err != nil {
				return 0, err
			}
		}
	}
	return off, nil
}

func skipBytes(data []byte, off, n int) (int, error) {
	off += n
	if off > len(data) {
		return len(data), nil
	}
	return off, nil
}

// skipSized reads a size header of sizeBytes width and skips that many data bytes.
// The declared length is read as uint64 so a 4-byte size near 2^32 cannot wrap
// a 32-bit int negative and slip past the bounds check.
func skipSized(data []byte, off, sizeBytes int) (int, error) {
	if off+sizeBytes > len(data) {
		return len(data), nil
	}
	var n uint64
	switch sizeBytes {
	case 1:
		n = uint64(data[off])
	case 2:
		n = uint64(binary.BigEndian.Uint16(data[off:]))
	case 4:
		n = uint64(binary.BigEndian.Uint32(data[off:]))
	}
	off += sizeBytes
	// len(data)-off is non-negative: off <= len(data) after the header bounds
	// check above, then off advanced by sizeBytes which that check accounted for.
	if n > uint64(len(data)-off) { // #nosec G115 -- len(data)-off is non-negative (see comment)
		return 0, fmt.Errorf("%w: blob declares %d bytes, only %d remain", ErrOversizedBlob, n, len(data)-off)
	}
	return skipBytes(data, off, int(n))
}

// skipExtSized reads a size header, then skips type(1) + size data bytes.
func skipExtSized(data []byte, off, sizeBytes int) (int, error) {
	if off+sizeBytes > len(data) {
		return len(data), nil
	}
	var n uint64
	switch sizeBytes {
	case 1:
		n = uint64(data[off])
	case 2:
		n = uint64(binary.BigEndian.Uint16(data[off:]))
	case 4:
		n = uint64(binary.BigEndian.Uint32(data[off:]))
	}
	off += sizeBytes
	// ext payload is type(1) + n data bytes; same OOM reasoning as skipSized.
	// len(data)-off is non-negative (off <= len(data) after the bounds check).
	if n+1 > uint64(len(data)-off) { // #nosec G115 -- len(data)-off is non-negative (see comment)
		return 0, fmt.Errorf("%w: ext declares %d bytes, only %d remain", ErrOversizedBlob, n, len(data)-off)
	}
	return skipBytes(data, off, int(n)+1) // +1 for ext type byte
}
