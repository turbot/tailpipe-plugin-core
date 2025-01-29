package sources

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/elastic/go-grok"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/filter"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*FileSource]()
}

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
	// TODO KAI BAD SOURCE CONFIG GIVES NO ERROR

	// get the layout
	layout := typehelpers.SafeString(s.Config.FileLayout)
	// if there are any optional segments, we expand them into all possible alternatives
	optionalLayouts := artifact_source.ExpandPatternIntoOptionalAlternatives(layout)

	var filterMap = make(map[string]*filter.SqlFilter)

	g := grok.New()
	// add any patterns defined in config
	err := g.AddPatterns(s.Config.GetPatterns())
	if err != nil {
		return fmt.Errorf("error adding grok patterns: %v", err)
	}

	var errList []error
	for _, basePath := range s.Config.Paths {
		err := filepath.WalkDir(basePath, func(targetPath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return s.WalkNode(ctx, targetPath, basePath, optionalLayouts, d.IsDir(), g, filterMap)
		})
		if err != nil {
			errList = append(errList, err)
		}

	}
	if len(errList) > 0 {
		return errors.Join(errList...)
	}
	return nil
}

// DownloadArtifact does nothing as the artifact already exists on the local file system
func (s *FileSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// for file source, the local name is the same as the name
	localName := info.Name
	fileInfo, err := os.Stat(localName)
	if err != nil {
		return fmt.Errorf("error getting file info for %s: %v", localName, err)
	}
	downloadInfo := types.NewDownloadedArtifactInfo(info, localName, fileInfo.Size())

	// notify observers of the downloaded artifact
	return s.OnArtifactDownloaded(ctx, downloadInfo)
}
