package sources

import (
	"context"
	"errors"
	"fmt"
	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/filter"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"io/fs"
	"path/filepath"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*FileSource]()
}

type FileSource struct {
	artifact_source.ArtifactSourceImpl[*FileSourceConfig, *artifact_source.EmptyConnection]

	Paths []string
}

func (s *FileSource) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, params, opts...); err != nil {
		return err
	}

	for _, p := range s.Config.Paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return fmt.Errorf("error getting absolute path for %s: %v", p, err)
		}
		s.Paths = append(s.Paths, abs)
	}

	return nil
}

func (s *FileSource) Identifier() string {
	return FileSourceIdentifier
}

func (s *FileSource) DiscoverArtifacts(ctx context.Context) error {
	// TODO KAI BAD SOURCE CONFIG GIVES NO ERROR

	// if we have a layout, check whether this is a directory we should descend into
	layout := s.Config.GetFileLayout()

	var filterMap = make(map[string]*filter.SqlFilter)

	g := grok.New()
	// add any patterns defined in config
	err := g.AddPatterns(s.Config.GetPatterns())
	if err != nil {
		return fmt.Errorf("error adding grok patterns: %v", err)
	}

	var errList []error
	for _, basePath := range s.Paths {
		err := filepath.WalkDir(basePath, func(targetPath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return s.WalkNode(ctx, targetPath, basePath, layout, d.IsDir(), g, filterMap)
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

// get the metadata from the given file path, based on the file layout
// returns whether the path matches the layout pattern, and the medata map
func (s *FileSource) getPathMetadata(g *grok.Grok, basePath, targetPath string, layout *string, isDir bool) (bool, map[string]string, error) {
	if layout == nil {
		return false, nil, nil
	}
	// remove the base path from the path
	relPath, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false, nil, err
	}
	var metadata map[string][]byte
	var match bool
	// if this is a directory, we just want to evaluate the pattern segments up to this directory
	// so call GetPathSegmentMetadata which trims the pattern to match the path length
	if isDir {
		match, metadata, err = artifact_source.GetPathSegmentMetadata(g, relPath, *layout)
	} else {
		match, metadata, err = artifact_source.GetPathMetadata(g, relPath, *layout)
	}
	if err != nil {
		return false, nil, err
	}

	// convert the metadata to a string map
	return match, ByteMapToStringMap(metadata), nil
}

func ByteMapToStringMap(m map[string][]byte) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		res[k] = string(v)
	}
	return res
}

// DownloadArtifact does nothing as the artifact already exists on the local file system
func (s *FileSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	// TODO JUST SET LOCAL NAME???? We need to review usage first
	// notify observers of the discovered artifact
	// NOTE: just pass on the info as is
	// if the file was downloaded we would update the Name to the local path, leaving OriginalName as the source path
	// TODO CREATE collection state data https://github.com/turbot/tailpipe-plugin-sdk/issues/11
	return s.OnArtifactDownloaded(ctx, info)
}

//func (s *FileSource) getFilterMap(granularity time.Duration) (map[string]*filter.SqlFilter, error) {
//	// If FromTime is zero, return an empty filter map
//	if s.FromTime.IsZero() {
//		return make(map[string]*filter.SqlFilter), nil
//	}
//
//	// Initialize the filters slice
//	filters := []string{
//		fmt.Sprintf("year > %d", s.FromTime.Year()),
//	}
//
//	// Add filters based on granularity
//	if granularity <= time.Hour*24*30 {
//		filters = append(filters, fmt.Sprintf("month > %d", int(s.FromTime.Month())))
//	}
//	if granularity <= time.Hour*24 {
//		filters = append(filters, fmt.Sprintf("day > %d", s.FromTime.Day()))
//	}
//	if granularity <= time.Hour {
//		filters = append(filters, fmt.Sprintf("hour > %d", s.FromTime.Hour()))
//	}
//	if granularity <= time.Minute {
//		filters = append(filters, fmt.Sprintf("minute > %d", s.FromTime.Minute()))
//	}
//	if granularity <= time.Second {
//		filters = append(filters, fmt.Sprintf("second > %d", s.FromTime.Second()))
//	}
//
//	// Build the filter map
//	filterMap, err := helpers.BuildFilterMap(filters)
//	if err != nil {
//		return nil, fmt.Errorf("failed to build filter map: %w", err)
//	}
//
//	return filterMap, nil
//}
