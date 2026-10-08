package service

import (
	"testing"
	"time"
)

func TestValidatePlatformConfig_GoogleRejectRequiresCheckEnabled(t *testing.T) {
	cfg := platformConfig{
		ReverseProxyEmptyAccountBehavior: "RANDOM",
		GoogleCheckEnabled:               false,
		GoogleRejectSentToChina:          true,
	}
	if err := validatePlatformConfig(&cfg, false); err == nil {
		t.Fatal("expected validation error when rejection is enabled without checking")
	}

	cfg.GoogleCheckEnabled = true
	if err := validatePlatformConfig(&cfg, false); err != nil {
		t.Fatalf("enabled Google check should satisfy dependency: %v", err)
	}
	if got := time.Duration(cfg.GoogleCheckIntervalNs); got != defaultGoogleCheckInterval {
		t.Fatalf("default interval: got %v want %v", got, defaultGoogleCheckInterval)
	}
}

func TestSetPlatformGoogleCheckInterval(t *testing.T) {
	cfg := platformConfig{}
	if err := setPlatformGoogleCheckInterval(&cfg, 30*time.Second); err == nil {
		t.Fatal("expected intervals shorter than one minute to be rejected")
	}
	if err := setPlatformGoogleCheckInterval(&cfg, 6*time.Hour); err != nil {
		t.Fatalf("set interval: %v", err)
	}
	if got := time.Duration(cfg.GoogleCheckIntervalNs); got != 6*time.Hour {
		t.Fatalf("interval: got %v want %v", got, 6*time.Hour)
	}
}
