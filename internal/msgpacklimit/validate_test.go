package msgpacklimit

import (
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestValidate_ValidInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"int", 42},
		{"string", "hello"},
		{"bool", true},
		{"float", 3.14},
		{"bytes", []byte("binary data")},
		{"small_array", []int{1, 2, 3}},
		{"small_map", map[string]int{"a": 1, "b": 2}},
		{"nested", map[string]any{"arr": []int{1, 2}, "str": "hi"}},
		{"empty_array", []int{}},
		{"empty_map", map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := msgpack.Marshal(tt.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := Validate(data); err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestValidate_OversizedArray(t *testing.T) {
	t.Parallel()
	// Craft a msgpack array32 header claiming 2 billion elements
	// 0xdd = array32, followed by 4-byte big-endian length
	data := []byte{0xdd, 0x77, 0x35, 0x94, 0x00} // ~2 billion
	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}

func TestValidate_OversizedMap(t *testing.T) {
	t.Parallel()
	// Craft a msgpack map32 header claiming 2 billion elements
	data := []byte{0xdf, 0x77, 0x35, 0x94, 0x00}
	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}

func TestValidate_NestedOversizedArray(t *testing.T) {
	t.Parallel()
	// fixarray(1) containing an array32 with huge length
	data := []byte{
		0x91,                         // fixarray of 1 element
		0xdd, 0x77, 0x35, 0x94, 0x00, // array32, ~2 billion
	}
	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}

func TestValidate_CrashInput(t *testing.T) {
	t.Parallel()
	// The actual crash input from ClusterFuzzLite: 118x 0xdd + 0xc3 0xc3 0x81 0x81 0xc3
	data := make([]byte, 123)
	for i := 0; i < 118; i++ {
		data[i] = 0xdd
	}
	data[118] = 0xc3
	data[119] = 0xc3
	data[120] = 0x81
	data[121] = 0x81
	data[122] = 0xc3

	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}

func TestValidate_EmptyInput(t *testing.T) {
	t.Parallel()
	if err := Validate(nil); err != nil {
		t.Errorf("Validate(nil) = %v, want nil", err)
	}
	if err := Validate([]byte{}); err != nil {
		t.Errorf("Validate([]) = %v, want nil", err)
	}
}

func TestValidate_TruncatedInput(t *testing.T) {
	t.Parallel()
	// Truncated array32 header (missing length bytes)
	if err := Validate([]byte{0xdd, 0x00}); err != nil {
		t.Errorf("Validate(truncated) = %v, want nil", err)
	}
}

func TestValidate_CumulativeBudgetExceeded(t *testing.T) {
	t.Parallel()
	// Chain of nested array32 headers each declaring 99,999 elements (under per-collection limit of 100K).
	// 11 such headers = 1,099,989 total elements > maxTotalElements (1M).
	// Each array32 header: 0xdd + 4-byte big-endian length
	data := make([]byte, 0, 55)
	for i := 0; i < 11; i++ {
		// array32 with 99999 = 0x0001869F
		data = append(data, 0xdd, 0x00, 0x01, 0x86, 0x9F)
	}
	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}

func TestValidate_CumulativeBudgetOK(t *testing.T) {
	t.Parallel()
	// 2 nested array32 headers each declaring 99,999 elements = 199,998 total (under 1M).
	data := []byte{
		0xdd, 0x00, 0x01, 0x86, 0x9F, // array32(99999)
		0xdd, 0x00, 0x01, 0x86, 0x9F, // nested array32(99999)
	}
	err := Validate(data)
	if err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestValidate_PerCollectionLimitExceeded(t *testing.T) {
	t.Parallel()
	// array32 with 200,000 elements (over per-collection limit of 100K)
	data := []byte{0xdd, 0x00, 0x03, 0x0D, 0x40} // 200000
	err := Validate(data)
	if !errors.Is(err, ErrOversizedCollection) {
		t.Errorf("Validate() = %v, want ErrOversizedCollection", err)
	}
}
