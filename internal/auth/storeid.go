package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	aessiv "github.com/jedisct1/go-aes-siv"
	"golang.org/x/crypto/hkdf"
)

// StoreIDVersion is the current version prefix for encrypted store IDs.
const StoreIDVersion = "v1"

// AADPrefix is the additional authenticated data prefix used during encryption
// to bind store IDs to their version and customer context.
const AADPrefix = "storeid:v1:"

// KeyInfoPrefix is the HKDF info string prefix used when deriving per-customer
// encryption keys from the master key.
const KeyInfoPrefix = "storeid:v1:enc:"

// KeySize is the required length in bytes for master encryption keys.
const KeySize = 32

// ErrInvalidStoreID indicates the store ID could not be parsed or decrypted,
// either due to malformed format, wrong customer, or tampering.
var ErrInvalidStoreID = errors.New("invalid store ID")

// ErrUnknownKeyID is returned when attempting to decrypt with a key ID
// that is not present in the current key set.
var ErrUnknownKeyID = errors.New("unknown key ID")

// ErrDecryptionError indicates the ciphertext failed authentication,
// typically meaning the data was corrupted or the wrong key was used.
var ErrDecryptionError = errors.New("decryption failed")

// ErrInvalidKey is returned when a provided encryption key has the wrong size
// or is otherwise unsuitable for use.
var ErrInvalidKey = errors.New("invalid key")

var (
	sitePattern     = regexp.MustCompile(`^[a-z0-9-]{1,16}$`)
	shardIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)
	uniqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16}$`)
	customerPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	keyIDPattern    = regexp.MustCompile(`^[0-9a-z][0-9a-z_-]{0,15}$`)
)

// StoreIDComponents contains the decrypted parts of a store ID.
type StoreIDComponents struct {
	Site     string
	ShardID  string
	UniqueID string
}

// StoreIDCipher encrypts and decrypts store IDs using AES-SIV with
// per-customer key derivation. This ensures store IDs are opaque to clients
// and bound to specific customers.
type StoreIDCipher interface {
	// Seal encrypts the store components into an opaque store ID token.
	Seal(site, shardID, unique, customerID string) (string, error)
	// Open decrypts and validates a store ID, returning its components.
	// Returns ErrInvalidStoreID if decryption fails or customer doesn't match.
	Open(storeID, customerID string) (*StoreIDComponents, error)
}

// KeySet manages a set of encryption keys for store ID encryption.
// Supports key rotation: new encryptions use currentID, but decryption
// works with any key in the set.
type KeySet struct {
	keys      map[string][]byte
	currentID string
}

// NewKeySet creates a KeySet from a map of key IDs to 32-byte keys.
func NewKeySet(keys map[string][]byte, currentID string) (*KeySet, error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys provided")
	}
	if _, ok := keys[currentID]; !ok {
		return nil, fmt.Errorf("current key ID %q not found in key set", currentID)
	}
	for id, key := range keys {
		if len(key) != KeySize {
			return nil, fmt.Errorf("key %q has invalid length %d, expected %d", id, len(key), KeySize)
		}
	}
	return &KeySet{keys: keys, currentID: currentID}, nil
}

// ParseKeyConfig parses key configuration from strings.
// Format: "id:hexkey,id2:hexkey2,..." or "dev" for development keys.
// If currentStr is empty, the lexicographically highest key ID is used.
func ParseKeyConfig(keysStr, currentStr string) (*KeySet, error) {
	keysStr = strings.TrimSpace(keysStr)
	if keysStr == "" {
		return nil, errors.New("empty key configuration")
	}

	if keysStr == "dev" {
		return DevKeySet(), nil
	}

	keys := make(map[string][]byte)
	for _, part := range strings.Split(keysStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		colonIdx := strings.Index(part, ":")
		if colonIdx == -1 {
			return nil, fmt.Errorf("invalid key format: %q (expected 'id:hexkey')", part)
		}
		keyID := part[:colonIdx]
		hexKey := part[colonIdx+1:]

		if !keyIDPattern.MatchString(keyID) {
			return nil, fmt.Errorf("invalid key ID: %q (must be 1-16 chars: 0-9, a-z, _, -)", keyID)
		}

		keyBytes, err := hex.DecodeString(hexKey)
		if err != nil {
			return nil, fmt.Errorf("invalid hex key for ID %q: %w", keyID, err)
		}
		if len(keyBytes) != KeySize {
			return nil, fmt.Errorf("key %q has invalid length %d bytes, expected %d", keyID, len(keyBytes), KeySize)
		}

		keys[keyID] = keyBytes
	}

	if len(keys) == 0 {
		return nil, errors.New("no valid keys parsed")
	}

	var currentID string
	if currentStr == "" {
		// Pick highest key ID (lexicographically) for deterministic behavior
		for id := range keys {
			if id > currentID {
				currentID = id
			}
		}
	} else {
		currentStr = strings.TrimSpace(currentStr)
		if !keyIDPattern.MatchString(currentStr) {
			return nil, fmt.Errorf("invalid current key ID: %q", currentStr)
		}
		currentID = currentStr
	}

	return NewKeySet(keys, currentID)
}

// DevKeySet returns a deterministic key set for development/testing.
// Not for production use.
func DevKeySet() *KeySet {
	devKey := make([]byte, KeySize)
	for i := range devKey {
		devKey[i] = byte(i)
	}
	return &KeySet{
		keys:      map[string][]byte{"0": devKey},
		currentID: "0",
	}
}

func (ks *KeySet) deriveKey(keyID string, customerID string) ([]byte, error) {
	master, ok := ks.keys[keyID]
	if !ok {
		return nil, ErrUnknownKeyID
	}

	info := []byte(KeyInfoPrefix + customerID)
	reader := hkdf.New(sha256.New, master, nil, info)
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("HKDF key derivation failed: %w", err)
	}
	return key, nil
}

// CurrentKeyID returns the key ID that will be used for new encryptions.
// Decryption can still use any key in the set, but this is the active one.
func (ks *KeySet) CurrentKeyID() string {
	return ks.currentID
}

type cipherImpl struct {
	keySet *KeySet
}

// NewCipher creates a StoreIDCipher that uses the provided key set for
// encrypting and decrypting store IDs. The cipher handles per-customer
// key derivation internally.
func NewCipher(ks *KeySet) StoreIDCipher {
	return &cipherImpl{keySet: ks}
}

func (c *cipherImpl) Seal(site, shardID, unique, customerID string) (string, error) {
	if !sitePattern.MatchString(site) {
		return "", fmt.Errorf("invalid site: %q", site)
	}
	if !shardIDPattern.MatchString(shardID) {
		return "", fmt.Errorf("invalid shardID: %q", shardID)
	}
	if !uniqueIDPattern.MatchString(unique) {
		return "", fmt.Errorf("invalid unique: %q", unique)
	}
	if !customerPattern.MatchString(customerID) {
		return "", fmt.Errorf("invalid customerID: %q", customerID)
	}

	keyID := c.keySet.currentID
	derivedKey, err := c.keySet.deriveKey(keyID, customerID)
	if err != nil {
		return "", err
	}

	aead, err := aessiv.New(derivedKey)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	plaintext := fmt.Sprintf("%s:%s:%s", site, shardID, unique)
	aad := []byte(AADPrefix + customerID)
	ciphertext := aead.Seal(nil, nil, []byte(plaintext), aad)

	encoded := base64.RawURLEncoding.EncodeToString(ciphertext)
	return fmt.Sprintf("%s:%s:%s", StoreIDVersion, keyID, encoded), nil
}

func (c *cipherImpl) Open(storeID, customerID string) (*StoreIDComponents, error) {
	if !customerPattern.MatchString(customerID) {
		return nil, ErrInvalidStoreID
	}

	parts := strings.SplitN(storeID, ":", 3)
	if len(parts) != 3 {
		return nil, ErrInvalidStoreID
	}

	version := parts[0]
	keyIDStr := parts[1]
	encodedCiphertext := parts[2]

	if version != StoreIDVersion {
		return nil, ErrInvalidStoreID
	}
	if !keyIDPattern.MatchString(keyIDStr) {
		return nil, ErrInvalidStoreID
	}
	keyID := keyIDStr

	ciphertext, err := base64.RawURLEncoding.DecodeString(encodedCiphertext)
	if err != nil {
		return nil, ErrInvalidStoreID
	}

	derivedKey, err := c.keySet.deriveKey(keyID, customerID)
	if err != nil {
		return nil, ErrInvalidStoreID
	}

	aead, err := aessiv.New(derivedKey)
	if err != nil {
		return nil, ErrInvalidStoreID
	}

	aad := []byte(AADPrefix + customerID)
	plaintext, err := aead.Open(nil, nil, ciphertext, aad)
	if err != nil {
		return nil, ErrInvalidStoreID
	}

	plaintextParts := strings.SplitN(string(plaintext), ":", 3)
	if len(plaintextParts) != 3 {
		return nil, ErrInvalidStoreID
	}

	site := plaintextParts[0]
	shardID := plaintextParts[1]
	unique := plaintextParts[2]

	if !sitePattern.MatchString(site) {
		return nil, ErrInvalidStoreID
	}
	if !shardIDPattern.MatchString(shardID) {
		return nil, ErrInvalidStoreID
	}
	if !uniqueIDPattern.MatchString(unique) {
		return nil, ErrInvalidStoreID
	}

	return &StoreIDComponents{
		Site:     site,
		ShardID:  shardID,
		UniqueID: unique,
	}, nil
}
