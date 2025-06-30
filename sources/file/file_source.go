package file

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/elastic/go-grok"

	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/filter"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type FileSource struct {
	artifact_source.ArtifactSourceImpl[*FileSourceConfig, *artifact_source.EmptyConnection]
}

func (s *FileSource) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, params, opts...); err != nil {
		return err
	}

	// ensure all paths are absolute
	for i, p := range s.Config.Paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return fmt.Errorf("error getting absolute path for %s: %v", p, err)
		}
		s.Config.Paths[i] = abs
	}

	return nil
}

func (s *FileSource) Identifier() string {
	return FileSourceIdentifier
}

func (s *FileSource) DiscoverArtifacts(ctx context.Context) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// TODO Bad source config gives no errors https://github.com/turbot/tailpipe-plugin-sdk/issues/244

	// get the layout
	layout := typehelpers.SafeString(s.Config.FileLayout)
	// if there are any optional segments, we expand them into all possible alternatives
	optionalLayouts := artifact_source.ExpandPatternIntoOptionalAlternatives(layout)

	var filterMap = make(map[string]*filter.SqlFilter)

	slog.Info("FileSource.DiscoverArtifacts", "layout", layout, "optionalLayouts", optionalLayouts)

	g := grok.New()
	// add any patterns defined in config
	err = g.AddPatterns(s.Config.GetPatterns())
	if err != nil {
		// this is a fatal error, log and return it
		slog.Error("FileSource.DiscoverArtifacts error adding grok patterns", "error", err)
		return fmt.Errorf("error adding grok patterns: %v", err)
	}

	for _, basePath := range s.Config.Paths {
		// TODO we need a mechanism to stop walking when weh have reached the 'To' time https://github.com/turbot/tailpipe-plugin-sdk/issues/243
		err := filepath.WalkDir(basePath, func(targetPath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return s.WalkNode(ctx, targetPath, basePath, optionalLayouts, d.IsDir(), g, filterMap)
		})
		if err != nil {
			// obtain error will be in format "op path: error"
			errStr := err.Error()

			// non-fatal error, log, notify and then attempt next path
			slog.Error("FileSource.DiscoverArtifacts error walking file path", "path", basePath, "error", errStr)

			// remove op from error string
			parts := strings.Fields(errStr)
			if len(parts) > 1 {
				errStr = strings.Join(parts[1:], " ")
			}

			s.NotifyError(ctx, executionId, errors.New(errStr))
			// reset err as handled
			err = nil
		}
	}

	slog.Info("FileSource.DiscoverArtifacts complete")
	return nil
}

// DownloadArtifact does nothing as the artifact already exists on the local file system
func (s *FileSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// for file source, the local name is the same as the name
	localName := info.Name
	fileName := filepath.Base(localName)

	// ensure file exists (this check can pass without error even if we cannot read the file)
	fileInfo, err := os.Stat(localName)
	if err != nil {
		slog.Error("FileSource.DownloadArtifact error obtaining file info", "file", localName, "error", err)
		return fmt.Errorf("%s: unable to obtain file info", fileName)
	}

	// ensure we can read the file
	f, err := os.Open(localName)
	if err != nil {
		slog.Error("FileSource.DownloadArtifact error opening file", "file", localName, "error", err)
		return fmt.Errorf("%s: unable to open file", fileName)
	}
	f.Close()

	// notify observers of the downloaded artifact
	return s.OnArtifactDownloaded(ctx, types.NewDownloadedArtifactInfo(info, localName, fileInfo.Size()))
}
