package standpipe

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/skillian/errors"
)

var (
	// Magic is the magic string found at the beginning of every
	// standpipe file.
	Magic = [...]byte{'S', 't', 'n', 'd', 'P', 'i', 'p', 'e'}

	// byteOrder determines the byte order that multi-byte numerics are
	// stored as.  The whole implementation expects a particular byte order.
	// If you change this, existing files with some other endianness will
	// break.
	byteOrder = binary.BigEndian
)

// Version describes a version
type Version struct {
	Major   int16
	Minor   int16
	Release int16
	Build   int16
}

// HeaderCommon is how every standpipe file starts.
type HeaderCommon struct {
	Magic [8]byte
	Version
}

// V1Header is the header format of version 1 files.
type V1Header struct {
	HeaderCommon

	// PageSize defines the size of every page within the standpipe file.
	PageSize int64

	// TableOffset is the offset into the standpipe file of the ordered list
	// of offsets with pages of data.
	TableOffset int64

	// TableLength is the length of the table of offsets in bytes.
	TableLength int64

	// ReadBufferOffset is the offset that the incomplete read buffer must
	// read from.
	ReadBufferOffset int64

	// ReadBufferIndex stores the index into the ReadBuffer where reading
	// stopped last.
	ReadBufferIndex int64

	// LastBufferLength holds the length of the last buffer in the
	// standpipe.  All other buffers are exactly PageSize bytes long but
	// the last one will probably not be the right size.
	LastBufferLength int64
}

const v1HeaderSize = unsafe.Sizeof(V1Header{})

// Unmarshal a header from bytes.
func Unmarshal(p []byte, t interface{}) error {
	_, err := unmarshal(p, t)
	return err
}

// unmarshal is a crude binary unmarshaler.  It only needs to handle
// unmarshaling values from standpipe headers.
func unmarshal(p []byte, t interface{}) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	switch v := t.(type) {
	case *int:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return unmarshal(p, (*uint)(unsafe.Pointer(v)))
	case *uint:
		var u64 uint64
		n, err = unmarshal(p, &u64)
		*v = uint(u64)
		return
	case *int8:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return unmarshal(p, (*uint8)(unsafe.Pointer(v)))
	case *uint8:
		n = 1
		*v = p[0]
		return
	case *int16:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return unmarshal(p, (*uint16)(unsafe.Pointer(v)))
	case *uint16:
		n = 2
		if err = checkLength(t, p, n); err != nil {
			return
		}
		*v = byteOrder.Uint16(p[:n])
		return
	case *int32:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return unmarshal(p, (*uint32)(unsafe.Pointer(v)))
	case *uint32:
		n = 4
		if err = checkLength(t, p, n); err != nil {
			return
		}
		*v = byteOrder.Uint32(p[:n])
		return
	case *int64:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return unmarshal(p, (*uint64)(unsafe.Pointer(v)))
	case *uint64:
		n = 8
		if err = checkLength(t, p, n); err != nil {
			return
		}
		*v = byteOrder.Uint64(p[:n])
		return
	default:
		rv := reflect.ValueOf(t)
		if rv.Kind() != reflect.Ptr {
			return 0, errors.Errorf(
				"can only unmarshal into a pointer.")
		}
		re := rv.Elem()
		switch re.Kind() {
		case reflect.Struct:
			limit := re.NumField()
			for i := 0; i < limit; i++ {
				f := re.Field(i)
				af := f.Addr()
				m, err := unmarshal(p[n:], af.Interface())
				if err != nil {
					return n, err
				}
				n += m
			}
			return
		case reflect.Array:
			if re.Type().Elem().Kind() != reflect.Uint8 {
				return 0, errors.Errorf(
					"can only handle byte arrays.")
			}
			m := re.Len()
			copy(re.Slice(0, m).Bytes(), p[n:])
			n += m
			return
		default:
			err = errors.Errorf(
				"cannot unmarshal %T", v)
			return
		}
	}
}

// Marshal the header into a byte slice.
func Marshal(v interface{}) ([]byte, error) {
	p := make([]byte, 0, int(reflect.ValueOf(v).Type().Size()))
	err := marshalInto(v, &p)
	return p, err
}

// marshalInto is a crude binary marshaler.  It only needs to handle
// marshaling values into standpipe headers.
func marshalInto(v interface{}, pp *[]byte) error {
	var bs [8]byte
	switch v := v.(type) {
	case int:
		i64 := int64(v)
		u64 := *((*uint64)(unsafe.Pointer(&i64)))
		return marshalInto(u64, pp)
	case uint:
		u64 := uint64(v)
		return marshalInto(u64, pp)
	case int8:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return marshalInto(*((*uint8)(unsafe.Pointer(&v))), pp)
	case uint8:
		*pp = append(*pp, v)
		return nil
	case int16:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return marshalInto(*((*uint16)(unsafe.Pointer(&v))), pp)
	case uint16:
		byteOrder.PutUint16(bs[:2], v)
		*pp = append(*pp, bs[:2]...)
		return nil
	case int32:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return marshalInto(*((*uint32)(unsafe.Pointer(&v))), pp)
	case uint32:
		byteOrder.PutUint32(bs[:4], v)
		*pp = append(*pp, bs[:4]...)
		return nil
	case int64:
		// Go requires negatives be two's complement, so this is
		// portable enough.
		return marshalInto(*((*uint64)(unsafe.Pointer(&v))), pp)
	case uint64:
		byteOrder.PutUint64(bs[:8], v)
		*pp = append(*pp, bs[:8]...)
		return nil
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Struct:
			limit := rv.NumField()
			for i := 0; i < limit; i++ {
				f := rv.Field(i)
				err := marshalInto(f.Interface(), pp)
				if err != nil {
					return err
				}
			}
			return nil
		case reflect.Array:
			if rv.Type().Elem().Kind() != reflect.Uint8 {
				return errors.Errorf(
					"can only handle byte arrays.")
			}
			*pp = append(*pp, reflectArrayToByteSlice(rv)...)
			return nil
		default:
			return errors.Errorf("cannot unmarshal %T", v)
		}
	}
}

func checkLength(t interface{}, p []byte, n int) error {
	if len(p) < n {
		return errors.Errorf(
			"cannot unmarshal %T from %d bytes", t, len(p))
	}
	return nil
}

// reflectArrayToByteSlice lets you get a reflect.Value of a byte array value
// as a slice.  This is not allowed by the reflect package unless the byte
// array is addressable but I don't care, so I'm doing this anyway.
func reflectArrayToByteSlice(a reflect.Value) []byte {
	x := *((*[3]uintptr)(unsafe.Pointer(&a)))
	h := reflect.SliceHeader{
		Data: x[1],
		Len:  a.Len(),
		Cap:  a.Cap(),
	}
	return *((*[]byte)(unsafe.Pointer(&h)))
}
