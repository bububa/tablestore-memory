package model

import (
	"testing"
)

func TestMetadata_PutValidation(t *testing.T) {
	m := NewMetadata()

	// Test that putting with empty key returns an error
	err := m.Put("", "value")
	if err == nil {
		t.Error("Expected error when putting with empty key, but got nil")
	}

	// Test that putting with nil value returns an error
	err = m.Put("key", nil)
	if err == nil {
		t.Error("Expected error when putting nil value, but got nil")
	}

	// Test that putting with valid key and value works
	err = m.Put("valid_key", "value")
	if err != nil {
		t.Errorf("Unexpected error when putting valid key and value: %v", err)
	}
}

func TestMetadata_GetBytes(t *testing.T) {
	m := NewMetadata()

	// Test getting non-existent key
	b, err := m.GetBytes("non_existent")
	if err != nil {
		t.Errorf("Unexpected error when getting non-existent key: %v", err)
	}
	if b != nil {
		t.Errorf("Expected nil for non-existent key, got %v", b)
	}

	// Test getting wrong type
	err = m.Put("string_key", "string_value")
	if err != nil {
		t.Fatalf("Unexpected error when putting string: %v", err)
	}
	b, err = m.GetBytes("string_key")
	if err == nil {
		t.Error("Expected error when getting string as bytes, but got nil")
	}

	// Test getting bytes
	expected := []byte("test_bytes")
	err = m.PutBytes("bytes_key", expected)
	if err != nil {
		t.Fatalf("Unexpected error when putting bytes: %v", err)
	}
	b, err = m.GetBytes("bytes_key")
	if err != nil {
		t.Errorf("Unexpected error when getting bytes: %v", err)
	}
	if string(b) != string(expected) {
		t.Errorf("Expected %v, got %v", expected, b)
	}
}
