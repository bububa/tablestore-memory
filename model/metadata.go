package model

import (
	"fmt"
	"maps"
	"reflect"

	"github.com/spf13/cast"
)

// --------------------
// type
// --------------------

type Metadata map[string]any

// --------------------
// supported value types
// --------------------

var supportedValueTypes = map[reflect.Type]struct{}{
	reflect.TypeFor[string]():  {},
	reflect.TypeFor[int]():     {},
	reflect.TypeFor[int32]():   {},
	reflect.TypeFor[int64]():   {},
	reflect.TypeFor[float32](): {},
	reflect.TypeFor[float64](): {},
	reflect.TypeFor[bool]():    {},
	reflect.TypeFor[[]byte]():  {},
}

// --------------------
// constructors
// --------------------

func NewMetadata() Metadata {
	return make(Metadata)
}

func MetadataFromMap(m map[string]any) (Metadata, error) {
	if m == nil {
		return nil, fmt.Errorf("metadata must not be nil")
	}
	md := NewMetadata()
	for k, v := range m {
		md.Put(k, v)
	}
	return md, nil
}

func MetadataFrom(key, value string) Metadata {
	return NewMetadata().PutString(key, value)
}

// --------------------
// validation
// --------------------

func validate(key string, value any) {
	if key == "" {
		panic("metadata key must not be blank")
	}
	if value == nil {
		panic(fmt.Sprintf("metadata value for key '%s' must not be nil", key))
	}
}

func checkSupportedValueTypes(key string, value any) {
	if _, ok := supportedValueTypes[reflect.TypeOf(value)]; !ok {
		panic(fmt.Sprintf(
			"Metadata key '%s' has unsupported type '%T'. Supported types: %v",
			key,
			value,
			supportedTypeNames(),
		))
	}
}

func supportedTypeNames() []string {
	names := make([]string, 0, len(supportedValueTypes))
	for t := range supportedValueTypes {
		names = append(names, t.String())
	}
	return names
}

// --------------------
// getters (cast-based)
// --------------------

func (m Metadata) Get(key string) (any, bool) {
	v, ok := m[key]
	return v, ok
}

func (m Metadata) GetString(key string) *string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	s := cast.ToString(v)
	return &s
}

func (m Metadata) GetBool(key string) *bool {
	v, ok := m[key]
	if !ok {
		return nil
	}
	b := cast.ToBool(v)
	return &b
}

func (m Metadata) GetInt(key string) *int {
	v, ok := m[key]
	if !ok {
		return nil
	}
	i := cast.ToInt(v)
	return &i
}

func (m Metadata) GetInt64(key string) *int64 {
	v, ok := m[key]
	if !ok {
		return nil
	}
	i := cast.ToInt64(v)
	return &i
}

func (m Metadata) GetFloat64(key string) *float64 {
	v, ok := m[key]
	if !ok {
		return nil
	}
	f := cast.ToFloat64(v)
	return &f
}

func (m Metadata) GetBytes(key string) []byte {
	v, ok := m[key]
	if !ok {
		return nil
	}
	b, ok := v.([]byte)
	if !ok {
		panic(typeError(key, v, "[]byte"))
	}
	return b
}

// --------------------
// put methods
// --------------------

func (m Metadata) PutString(key, value string) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) PutBool(key string, value bool) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) PutInt(key string, value int) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) PutInt64(key string, value int64) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) PutFloat64(key string, value float64) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) PutBytes(key string, value []byte) Metadata {
	validate(key, value)
	m[key] = value
	return m
}

func (m Metadata) Put(key string, value any) Metadata {
	validate(key, value)
	checkSupportedValueTypes(key, value)
	m[key] = value
	return m
}

// --------------------
// misc
// --------------------

func (m Metadata) HasKey(key string) bool {
	_, ok := m[key]
	return ok
}

func (m Metadata) Remove(key string) Metadata {
	delete(m, key)
	return m
}

func (m Metadata) Size() int {
	return len(m)
}

func (m Metadata) Copy() Metadata {
	cp := make(Metadata, len(m))
	maps.Copy(cp, m)
	return cp
}

func (m Metadata) ToMap() map[string]any {
	return m.Copy()
}

func (m Metadata) Merge(another Metadata) Metadata {
	if len(another) == 0 {
		return m.Copy()
	}

	// Check for duplicate keys first to avoid partial merges
	for k := range another {
		if _, exists := m[k]; exists {
			panic(fmt.Sprintf("Metadata keys are not unique. Common key: %s", k))
		}
	}

	// If no duplicates, create result and merge
	result := m.Copy()
	for k, v := range another {
		result[k] = v
	}

	return result
}

func typeError(key string, value any, want string) error {
	return fmt.Errorf("metadata entry with key '%s' has value '%v' and type '%T' which cannot be returned as %s",
		key,
		value,
		value,
		want,
	)
}
