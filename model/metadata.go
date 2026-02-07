package model

import (
	"errors"
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

func MetadataFrom(key, value string) (Metadata, error) {
	m := NewMetadata()
	if err := m.PutString(key, value); err != nil {
		return nil, err
	}
	return m, nil
}

// --------------------
// validation
// --------------------

func validate(key string, value any) error {
	if key == "" {
		return errors.New("metadata key must not be blank")
	}
	if value == nil {
		return fmt.Errorf("metadata value for key '%s' must not be nil", key)
	}
	return nil
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

func (m Metadata) GetBytes(key string) ([]byte, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, typeError(key, v, "[]byte")
	}
	return b, nil
}

// --------------------
// put methods
// --------------------

func (m Metadata) PutString(key, value string) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) PutBool(key string, value bool) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) PutInt(key string, value int) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) PutInt64(key string, value int64) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) PutFloat64(key string, value float64) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) PutBytes(key string, value []byte) error {
	if err := validate(key, value); err != nil {
		return err
	}
	m[key] = value
	return nil
}

func (m Metadata) Put(key string, value any) error {
	if err := validate(key, value); err != nil {
		return err
	}
	v, err := toPrimitive(value)
	if err != nil {
		return fmt.Errorf(
			"Metadata key '%s' has unsupported type '%T'. Supported types: %v",
			key,
			value,
			supportedTypeNames(),
		)
	}
	m[key] = v
	return nil
}

func toPrimitive(value any) (any, error) {
	t := reflect.TypeOf(value)
	if _, ok := supportedValueTypes[t]; ok {
		return value, nil
	}
	rv := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.String:
		return rv.String(), nil
	case reflect.Int:
		return int(rv.Int()), nil
	case reflect.Int32:
		return int32(rv.Int()), nil
	case reflect.Int64:
		return rv.Int(), nil
	case reflect.Float32:
		return float32(rv.Float()), nil
	case reflect.Float64:
		return rv.Float(), nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return rv.Bytes(), nil
		}
	}
	return nil, errors.New("unsupported type")
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

func (m Metadata) Merge(another Metadata) (Metadata, error) {
	if len(another) == 0 {
		return m.Copy(), nil
	}

	// Check for duplicate keys first to avoid partial merges
	for k := range another {
		if _, exists := m[k]; exists {
			return nil, fmt.Errorf("Metadata keys are not unique. Common key: %s", k)
		}
	}

	// If no duplicates, create result and merge
	result := m.Copy()
	maps.Copy(result, another)

	return result, nil
}

func typeError(key string, value any, want string) error {
	return fmt.Errorf("metadata entry with key '%s' has value '%v' and type '%T' which cannot be returned as %s",
		key,
		value,
		value,
		want,
	)
}
