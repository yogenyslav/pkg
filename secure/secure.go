// Package secure provides methods for hashing and encryption.
package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrCipherTooShort reports cipher key shorter than 32 bits.
	ErrCipherTooShort = errors.New("cipher too short, must use 32-bit")
)

// Encrypt string with a given key.
func Encrypt(toEncrypt, keyString string) (string, error) {
	key, err := hex.DecodeString(keyString)
	if err != nil {
		return "", fmt.Errorf("encryption key: %w", err)
	}
	plain := []byte(toEncrypt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to initialize AES cipher: %w", err)
	}

	ciphertext := make([]byte, aes.BlockSize+len(plain))
	iv := ciphertext[:aes.BlockSize]
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		return "", fmt.Errorf("failed to generate iv: %w", err)
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plain)
	return base64.URLEncoding.EncodeToString(ciphertext), nil
}

// Decrypt a string with a given key.
func Decrypt(toDecrypt, keyString string) (string, error) {
	key, err := hex.DecodeString(keyString)
	if err != nil {
		return "", fmt.Errorf("decryption key: %w", err)
	}
	cipherText, err := base64.URLEncoding.DecodeString(toDecrypt)
	if err != nil {
		return "", fmt.Errorf("ciphered text: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("cipher block: %w", err)
	}
	if len(cipherText) < aes.BlockSize {
		return "", ErrCipherTooShort
	}

	iv := cipherText[:aes.BlockSize]
	cipherText = cipherText[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(cipherText, cipherText)
	return string(cipherText), nil
}
