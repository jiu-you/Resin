package service

import "testing"

func TestValidatePlatformConfig_AllowsGoogleRejectWithoutPlatformScheduler(t *testing.T) {
	cfg := platformConfig{
		ReverseProxyEmptyAccountBehavior: "RANDOM",
		GoogleRejectSentToChina:          true,
	}
	if err := validatePlatformConfig(&cfg, false); err != nil {
		t.Fatalf("Google rejection is a platform routing policy and should validate independently: %v", err)
	}
}
