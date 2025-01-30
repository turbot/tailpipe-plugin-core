package file

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type testObserver struct {
	Artifacts []string
}

func (t *testObserver) Notify(ctx context.Context, e events.Event) error {
	switch ty := e.(type) {
	case *events.ArtifactDiscovered:
		t.Artifacts = append(t.Artifacts, ty.Info.Name)
	}
	return nil
}

func TestFileSource_DiscoverArtifacts(t *testing.T) {
	type fields struct {
		paths      []string
		fileLayout string
		//config     []byte
	}

	tests := []struct {
		name              string
		fields            fields
		expectedArtifacts []string
		wantErr           bool
	}{
		{
			name: "pattern matches directory",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/%{WORD:org}/%{WORD:account_id}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
		{
			name: "pattern dos not match - one segment shorter directory",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/%{WORD:org}/%{WORD:account_id}/Cloud/%{WORD:region}/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{},
		},
		{
			name: "optional org with no org field in target path",
			fields: fields{
				paths:      []string{"./test_data/discover_test_no_org"},
				fileLayout: "AWSLogs/(%{WORD:org}/)?%{WORD:account_id}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_no_org/AWSLogs/1/CloudTrail/1_1.log",
				"./test_data/discover_test_no_org/AWSLogs/1/CloudTrail/1_2.log",
				"./test_data/discover_test_no_org/AWSLogs/2/CloudTrail/2_1.log",
				"./test_data/discover_test_no_org/AWSLogs/2/CloudTrail/2_2.log",
				"./test_data/discover_test_no_org/AWSLogs/3/CloudTrail/3_1.log",
				"./test_data/discover_test_no_org/AWSLogs/3/CloudTrail/3_2.log",
			},
		},
		{
			name: "optional org with org field in target path",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/(%{WORD:org}/)?%{WORD:account_id}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
		{
			name: "wildcard at end of path (with file pattern)",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/%{WORD:org}/%{DATA}/%{WORD}.log",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
		{
			name: "wildcard at end of path (NO file pattern)",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/%{WORD:org}/%{DATA}",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/4/CloudTrail/4_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/5/CloudTrail/5_3.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_1.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_2.log",
				"./test_data/discover_test_1/AWSLogs/org2/6/CloudTrail/6_3.log",
			},
		},
		{
			name: "pattern doe not match - file below pattern leaf",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/%{WORD:org}/%{WORD:account_id}/CloudTrail/%{WORD:region}/%{NOTSPACE:file_name}.%%{WORD:ext}",
			},
			expectedArtifacts: []string{},
		},
		{
			name: "no files",
			fields: fields{
				paths:      []string{"./test_data/discover_test_empty"},
				fileLayout: "AWSLogs/%{WORD:org}/%{WORD:account_id}/CloudTrail/%{WORD:region}/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{},
		},
		{
			name: "org1",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/org1/%{WORD:account_id}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/1/CloudTrail/1_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/2/CloudTrail/2_2.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_1.log",
				"./test_data/discover_test_1/AWSLogs/org1/3/CloudTrail/3_2.log",
			},
		},
		{
			name: "org4 - empty",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/org4/%{WORD:account_id}/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{},
		},
		{
			name: "org5 - does not exist",
			fields: fields{
				paths:      []string{"./test_data/discover_test_1"},
				fileLayout: "AWSLogs/org5/2/CloudTrail/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			expectedArtifacts: []string{},
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			ctx := context_values.WithExecutionId(context.Background(), "test")

			config := &FileSourceConfig{
				ArtifactSourceConfigImpl: artifact_source_config.ArtifactSourceConfigImpl{
					FileLayout: &tt.fields.fileLayout,
				},
				Paths: tt.fields.paths,
			}

			s, err := getFileSource(ctx, t, config)
			if err != nil {
				t.Fatalf("failed to get file source")
			}

			// add an observer to the source
			var observer testObserver
			_ = s.AddObserver(&observer)

			// do the discovery
			err = s.DiscoverArtifacts(ctx)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("DiscoverArtifacts() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if len(observer.Artifacts) != len(tt.expectedArtifacts) {
				t.Fatalf("DiscoverArtifacts() expected %v artifacts, got %v", len(tt.expectedArtifacts), len(observer.Artifacts))
			}
			for i, expected := range tt.expectedArtifacts {
				e, _ := filepath.Abs(expected)
				if observer.Artifacts[i] != e {
					t.Errorf("DiscoverArtifacts() expected artifact %v, got %v", expected, observer.Artifacts[i])
				}
			}
		})
	}
}

func getFileSource(ctx context.Context, t *testing.T, config *FileSourceConfig) (*FileSource, error) {

	s := &FileSource{}
	any(s).(row_source.BaseSource).RegisterSource(s)

	// use dummy hcl to satisfy the init process
	hclBytes := []byte(`paths = ["."]`)
	err := s.Init(ctx, &row_source.RowSourceParams{SourceConfigData: types.NewSourceConfigData(hclBytes, hcl.Range{}, "file")})
	if err != nil {
		t.Fatalf("failed to init")
	}

	// NOW set the config

	// set the config
	s.Config = config

	// ensure all paths are absolute
	for i, p := range s.Config.Paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, fmt.Errorf("error getting absolute path for %s: %v", p, err)
		}
		s.Config.Paths[i] = abs
	}

	return s, err
}
