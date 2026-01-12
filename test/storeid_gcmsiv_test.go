package test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/routing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	site := "local"
	shardID := "abcdefghijk"
	unique := "1234567890abcdef"
	customerID := "customer123"

	storeID, err := cipher.Seal(site, shardID, unique, customerID)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	components, err := cipher.Open(storeID, site, customerID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if components.Site != site {
		t.Errorf("site mismatch: got %q, want %q", components.Site, site)
	}
	if components.ShardID != shardID {
		t.Errorf("shardID mismatch: got %q, want %q", components.ShardID, shardID)
	}
	if components.UniqueID != unique {
		t.Errorf("uniqueID mismatch: got %q, want %q", components.UniqueID, unique)
	}
}

func TestWrongCustomerIDFails(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	storeID, err := cipher.Seal("local", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Wrong customer should fail - key derivation uses customer, so decryption will fail
	_, err = cipher.Open(storeID, "local", "customer2")
	if err == nil {
		t.Fatal("Open should fail with wrong customerID")
	}
	if err != auth.ErrInvalidStoreID {
		t.Errorf("expected ErrInvalidStoreID, got: %v", err)
	}
}

func TestTamperedCiphertextFails(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	storeID, err := cipher.Seal("local", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	tampered := storeID[:len(storeID)-4] + "XXXX"

	_, err = cipher.Open(tampered, "local", "customer1")
	if err == nil {
		t.Fatal("Open should fail with tampered ciphertext")
	}
}

func TestTamperedKeyIDFails(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	storeID, err := cipher.Seal("local", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	tampered := "v1:x:" + storeID[5:]

	_, err = cipher.Open(tampered, "local", "customer1")
	if err == nil {
		t.Fatal("Open should fail with unknown key ID")
	}
}

func TestKeyRotation(t *testing.T) {
	key0 := make([]byte, 32)
	key1 := make([]byte, 32)
	for i := range key0 {
		key0[i] = byte(i)
		key1[i] = byte(i + 100)
	}

	oldKeys, _ := auth.NewKeySet(map[string][]byte{"0": key0}, "0")
	oldCipher := auth.NewCipher(oldKeys)

	storeID, err := oldCipher.Seal("local", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal with old key failed: %v", err)
	}

	newKeys, _ := auth.NewKeySet(map[string][]byte{"0": key0, "1": key1}, "1")
	newCipher := auth.NewCipher(newKeys)

	components, err := newCipher.Open(storeID, "local", "customer1")
	if err != nil {
		t.Fatalf("Open with new keyset failed: %v", err)
	}

	if components.Site != "local" {
		t.Errorf("site mismatch: got %q", components.Site)
	}
}

func TestFieldValidation(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	tests := []struct {
		name       string
		site       string
		shardID    string
		unique     string
		customerID string
		wantErr    bool
	}{
		{"valid", "local", "abcdefghijk", "1234567890abcdef", "cust1", false},
		{"site too long", "thissiteistoolongforvalidation", "abcdefghijk", "1234567890abcdef", "cust1", true},
		{"site uppercase", "LOCAL", "abcdefghijk", "1234567890abcdef", "cust1", true},
		{"site empty", "", "abcdefghijk", "1234567890abcdef", "cust1", true},
		{"shardID wrong length", "local", "abc", "1234567890abcdef", "cust1", true},
		{"unique wrong length", "local", "abcdefghijk", "short", "cust1", true},
		{"customerID too long", "local", "abcdefghijk", "1234567890abcdef", "this_customer_id_is_way_too_long_for_the_maximum_allowed_length_limit", true},
		{"customerID empty", "local", "abcdefghijk", "1234567890abcdef", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cipher.Seal(tt.site, tt.shardID, tt.unique, tt.customerID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Seal() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseKeyConfig(t *testing.T) {
	tests := []struct {
		name       string
		keysStr    string
		currentStr string
		wantErr    bool
	}{
		{
			name:       "dev mode",
			keysStr:    "dev",
			currentStr: "",
			wantErr:    false,
		},
		{
			name:       "single key",
			keysStr:    "0:0001020304050607080910111213141516171819202122232425262728293031",
			currentStr: "0",
			wantErr:    false,
		},
		{
			name:       "multiple keys",
			keysStr:    "0:0001020304050607080910111213141516171819202122232425262728293031,1:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			currentStr: "1",
			wantErr:    false,
		},
		{
			name:       "default current key",
			keysStr:    "a:0001020304050607080910111213141516171819202122232425262728293031",
			currentStr: "",
			wantErr:    false,
		},
		{
			name:       "empty keys",
			keysStr:    "",
			currentStr: "",
			wantErr:    true,
		},
		{
			name:       "invalid key length",
			keysStr:    "0:001122",
			currentStr: "",
			wantErr:    true,
		},
		{
			name:       "invalid hex",
			keysStr:    "0:zzzz",
			currentStr: "",
			wantErr:    true,
		},
		{
			name:       "invalid key id",
			keysStr:    "AB:0001020304050607080910111213141516171819202122232425262728293031",
			currentStr: "",
			wantErr:    true,
		},
		{
			name:       "missing current key",
			keysStr:    "0:0001020304050607080910111213141516171819202122232425262728293031",
			currentStr: "9",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := auth.ParseKeyConfig(tt.keysStr, tt.currentStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseKeyConfig() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeterminism(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	site := "local"
	shardID := "abcdefghijk"
	unique := "1234567890abcdef"
	customerID := "customer123"

	storeID1, err := cipher.Seal(site, shardID, unique, customerID)
	if err != nil {
		t.Fatalf("Seal 1 failed: %v", err)
	}

	storeID2, err := cipher.Seal(site, shardID, unique, customerID)
	if err != nil {
		t.Fatalf("Seal 2 failed: %v", err)
	}

	if storeID1 != storeID2 {
		t.Errorf("determinism failed: %q != %q", storeID1, storeID2)
	}
}

func TestDifferentCustomersDifferentCiphertext(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	site := "local"
	shardID := "abcdefghijk"
	unique := "1234567890abcdef"

	storeID1, _ := cipher.Seal(site, shardID, unique, "customer1")
	storeID2, _ := cipher.Seal(site, shardID, unique, "customer2")

	if storeID1 == storeID2 {
		t.Error("different customers should produce different ciphertexts")
	}
}

func TestInvalidStoreIDFormat(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	tests := []struct {
		name    string
		storeID string
	}{
		{"empty", ""},
		{"no colons", "v1abcdef"},
		{"one colon", "v1:abcdef"},
		{"wrong version", "v2:0:abcdef"},
		{"invalid key id", "v1:AB:abcdef"},
		{"invalid base64", "v1:0:!!!invalid!!!"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cipher.Open(tt.storeID, "local", "customer1")
			if err == nil {
				t.Error("Open should fail for invalid store ID")
			}
		})
	}
}

func TestNewKeySetValidation(t *testing.T) {
	validKey := make([]byte, 32)
	shortKey := make([]byte, 16)

	_, err := auth.NewKeySet(nil, "0")
	if err == nil {
		t.Error("NewKeySet should fail with nil keys")
	}

	_, err = auth.NewKeySet(map[string][]byte{}, "0")
	if err == nil {
		t.Error("NewKeySet should fail with empty keys")
	}

	_, err = auth.NewKeySet(map[string][]byte{"0": shortKey}, "0")
	if err == nil {
		t.Error("NewKeySet should fail with short key")
	}

	_, err = auth.NewKeySet(map[string][]byte{"0": validKey}, "1")
	if err == nil {
		t.Error("NewKeySet should fail when currentID not in keys")
	}

	ks, err := auth.NewKeySet(map[string][]byte{"0": validKey}, "0")
	if err != nil {
		t.Errorf("NewKeySet failed with valid input: %v", err)
	}
	if ks.CurrentKeyID() != "0" {
		t.Errorf("CurrentKeyID() = %s, want '0'", ks.CurrentKeyID())
	}
}

func TestEncryptedStoreIDIntegration(t *testing.T) {
	// Create cipher with dev keys
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	// Generate an encrypted store ID
	site := "local"
	shardID := "abcdefghijk"
	customerID := "customer123"

	storeID, err := routing.GenerateEncryptedStoreID(cipher, site, shardID, customerID)
	if err != nil {
		t.Fatalf("GenerateEncryptedStoreID failed: %v", err)
	}

	// Verify it's encrypted format (v1:keyID:ciphertext)
	if !strings.HasPrefix(storeID, "v1:") {
		t.Errorf("store ID should start with v1:, got %s", storeID)
	}

	// Verify we can decrypt and get back the components
	components, err := cipher.Open(storeID, site, customerID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if components.Site != site {
		t.Errorf("site mismatch: got %q, want %q", components.Site, site)
	}
	if components.ShardID != shardID {
		t.Errorf("shardID mismatch: got %q, want %q", components.ShardID, shardID)
	}

	// Verify wrong customer can't decrypt
	_, err = cipher.Open(storeID, site, "wrong-customer")
	if err == nil {
		t.Error("should fail to decrypt with wrong customer")
	}
}

func TestHighestKeyIDSelectedByDefault(t *testing.T) {
	// Create multiple keys
	key0 := make([]byte, 32)
	key5 := make([]byte, 32)
	keya := make([]byte, 32)
	for i := range key0 {
		key0[i] = byte(i)
		key5[i] = byte(i + 50)
		keya[i] = byte(i + 100)
	}

	// Parse config without specifying current key
	ks, err := auth.ParseKeyConfig(
		"5:"+hex.EncodeToString(key5)+",0:"+hex.EncodeToString(key0)+",a:"+hex.EncodeToString(keya),
		"", // no current key specified
	)
	if err != nil {
		t.Fatalf("ParseKeyConfig failed: %v", err)
	}

	// Should pick 'a' as highest key ID
	if ks.CurrentKeyID() != "a" {
		t.Errorf("CurrentKeyID() = %q, want 'a'", ks.CurrentKeyID())
	}
}

func TestLongKeyIDs(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key1 {
		key1[i] = byte(i)
		key2[i] = byte(i + 100)
	}

	// Test with longer key IDs
	ks, err := auth.NewKeySet(map[string][]byte{
		"2024q1": key1,
		"2024q2": key2,
	}, "2024q2")
	if err != nil {
		t.Fatalf("NewKeySet with long key IDs failed: %v", err)
	}

	cipher := auth.NewCipher(ks)

	storeID, err := cipher.Seal("local", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Verify key ID is embedded in store ID
	if !strings.Contains(storeID, ":2024q2:") {
		t.Errorf("store ID should contain key ID '2024q2': %s", storeID)
	}

	// Verify decryption works
	components, err := cipher.Open(storeID, "local", "customer1")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if components.Site != "local" {
		t.Errorf("site mismatch: got %q", components.Site)
	}
}

func TestWrongSiteFails(t *testing.T) {
	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	// Create store ID for site-a
	storeID, err := cipher.Seal("site-a", "abcdefghijk", "1234567890abcdef", "customer1")
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Should succeed with correct site
	components, err := cipher.Open(storeID, "site-a", "customer1")
	if err != nil {
		t.Fatalf("Open with correct site failed: %v", err)
	}
	if components.Site != "site-a" {
		t.Errorf("site mismatch: got %q, want site-a", components.Site)
	}

	// Should fail with wrong site - key derivation includes site, so decryption will fail
	_, err = cipher.Open(storeID, "site-b", "customer1")
	if err == nil {
		t.Fatal("Open should fail with wrong site")
	}
	if err != auth.ErrInvalidStoreID {
		t.Errorf("expected ErrInvalidStoreID, got: %v", err)
	}
}

func TestKeyIDValidation(t *testing.T) {
	key := make([]byte, 32)

	tests := []struct {
		name    string
		keyID   string
		wantErr bool
	}{
		{"single char", "a", false},
		{"numeric", "0", false},
		{"long alphanumeric", "2024q1prod", false},
		{"with underscore", "key_v1", false},
		{"with hyphen", "key-v1", false},
		{"max length", "0123456789abcdef", false},
		{"too long", "01234567890abcdefg", true},
		{"uppercase", "KEY", true},
		{"starts with hyphen", "-key", true},
		{"starts with underscore", "_key", true},
		{"empty", "", true},
		{"special chars", "key@1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := auth.ParseKeyConfig(
				tt.keyID+":"+hex.EncodeToString(key),
				"",
			)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseKeyConfig() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
