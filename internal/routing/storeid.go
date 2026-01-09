package routing

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/dip-proto/bigbunny/internal/auth"
)

// GenerateUniqueID creates a cryptographically random 16-char base64url string.
func GenerateUniqueID() (string, error) {
	uniqueBytes := make([]byte, 12)
	if _, err := rand.Read(uniqueBytes); err != nil {
		return "", fmt.Errorf("generate unique id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(uniqueBytes), nil
}

// GenerateEncryptedStoreID creates a new store ID with a fresh unique component,
// encrypted and bound to the given customer.
func GenerateEncryptedStoreID(cipher auth.StoreIDCipher, site, shardID, customerID string) (string, error) {
	unique, err := GenerateUniqueID()
	if err != nil {
		return "", err
	}
	return cipher.Seal(site, shardID, unique, customerID)
}

// GenerateShardID creates a random shard ID for rendezvous hash routing.
func GenerateShardID() (string, error) {
	shardBytes := make([]byte, 8)
	if _, err := rand.Read(shardBytes); err != nil {
		return "", fmt.Errorf("generate shard id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(shardBytes), nil
}

// GenerateLockID creates a random lock token for the begin/complete modify flow.
func GenerateLockID() (string, error) {
	lockBytes := make([]byte, 16)
	if _, err := rand.Read(lockBytes); err != nil {
		return "", fmt.Errorf("generate lock id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(lockBytes), nil
}
