package priority_test

import (
	"testing"

	"github.com/yuninks/timerx/priority"
)


func TestVersionToPriority(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    int64
		wantErr bool
	}{
		{
			name:    "standard version",
			version: "1.2.3",
			want:    1002003000000,
			wantErr: false,
		},
		{
			name:    "version with v prefix",
			version: "v1.2.3",
			want:    1002003000000,
			wantErr: false,
		},
		{
			name:    "version with V prefix",
			version: "V1.2.3",
			want:    1002003000000,
			wantErr: false,
		},
		{
			name:    "single digit version",
			version: "5",
			want:    5000000000000,
			wantErr: false,
		},
		{
			name:    "max digits version",
			version: "999.999.999.999.999",
			want:    999999999999999,
			wantErr: false,
		},
		{
			name:    "empty version",
			version: "",
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid character",
			version: "1.a.3",
			want:    0,
			wantErr: true,
		},
		{
			name:    "zero version part",
			version: "1.0.3",
			want:    1000003000000,
			wantErr: false,
		},
		{
			name:    "zero version part 2",
			version: "1.0.3.",
			want:    0,
			wantErr: true,
		},
		{
			name:    "negative version part",
			version: "1.-2.3",
			want:    0,
			wantErr: true,
		},
		{
			name:    "version part too large",
			version: "1.1000.3",
			want:    0,
			wantErr: true,
		},
		{
			name:    "too many parts",
			version: "1.2.3.4.5.6",
			want:    0,
			wantErr: true,
		},
		{
			name:    "empty part",
			version: "1..3",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := priority.PriorityByVersion(tt.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("VersionToPriority() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("VersionToPriority() = %v, want %v", got, tt.want)
			}
		})
	}
}
