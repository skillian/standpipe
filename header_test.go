package standpipe_test

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/skillian/standpipe"
)

var (
	h1 = standpipe.V1Header{
		HeaderCommon: standpipe.HeaderCommon{
			Magic: standpipe.Magic,
			Version: standpipe.Version{
				Major:   0,
				Minor:   1,
				Release: 0,
				Build:   0,
			},
		},
		PageSize:         0x8000,
		TableOffset:      0x8765,
		TableLength:      0xfedc,
		ReadBufferOffset: 0xabcd,
	}

	h1Bytes = [...]byte{
		'S', 't', 'n', 'd', 'P', 'i', 'p', 'e', // Magic
		0x00, 0x00, // Version.Major
		0x00, 0x01, // Version.Minor
		0x00, 0x00, // Version.Release
		0x00, 0x00, // Version.Build
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, // PageSize
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x87, 0x65, // TableOffset
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfe, 0xdc, // TableLength
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0xcd, // ReadBufferOffset
	}
)

func TestMarshal(t *testing.T) {
	bs, err := standpipe.Marshal(h1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h1Bytes[:], bs) {
		t.Fatalf(
			"bytes:\n\n%v\n\ndo not equal bytes:\n\n%v\n",
			h1Bytes, bs)
	}
}

func TestUnmarshal(t *testing.T) {
	var v1 standpipe.V1Header
	err := standpipe.Unmarshal(h1Bytes[:], &v1)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != h1 {
		t.Fatalf(
			"header:\n\n%v\n\ndo not equal header:\n\n%v\n",
			h1, v1)
	}
}

func TestPreallocatedV1HeaderSliceSize(t *testing.T) {
	pre := int(unsafe.Sizeof(standpipe.V1Header{}))
	ok := pre == len(h1Bytes)
	if !ok {
		t.Error(
			"preallocated size:", pre,
			"does not match actual size:", len(h1Bytes))
	}
}
