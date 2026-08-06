package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func signature(secret, timestamp, method, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(method))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(path))
	if body != nil {
		_, _ = mac.Write([]byte("."))
		_, _ = mac.Write(body)
	}
	return "hmac-sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func webhookSignature(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return "hmac-sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func validSignature(got, want string) bool {
	gotHex, ok := strings.CutPrefix(got, "hmac-sha256=")
	if !ok {
		return false
	}
	gotBytes, err := hex.DecodeString(gotHex)
	if err != nil {
		return false
	}
	wantHex, _ := strings.CutPrefix(want, "hmac-sha256=")
	wantBytes, err := hex.DecodeString(wantHex)
	return err == nil && hmac.Equal(gotBytes, wantBytes)
}
