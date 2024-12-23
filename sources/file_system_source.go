package sources

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// register the source from the package init function
func init() {
	row_source.RegisterRowSource[*FileSystemSource]()
}

type FileSystemSource struct {
	artifact_source.ArtifactSourceImpl[*artifact_source_config.FileSystemSourceConfig, *artifact_source.EmptyConnection]
	Paths      []string
	Extensions types.ExtensionLookup
}

func (s *FileSystemSource) Init(ctx context.Context, configData, connectionData types.ConfigData, opts ...row_source.RowSourceOption) error {
	// call base to parse config and apply options
	if err := s.ArtifactSourceImpl.Init(ctx, configData, connectionData, opts...); err != nil {
		return err
	}

	s.Paths = s.Config.Paths
	s.Extensions = types.NewExtensionLookup(s.Config.Extensions)
	slog.Info("Initialized FileSystemSource", "paths", s.Paths, "extensions", s.Extensions)
	return nil
}

func (s *FileSystemSource) Identifier() string {
	return artifact_source_config.FileSystemSourceIdentifier
}

func (s *FileSystemSource) DiscoverArtifacts(ctx context.Context) error {
	var errList []error
	for _, path := range s.Paths {
		err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			// check the extension
			if s.Extensions.IsValid(path) {
				// populate enrichment fields the source is aware of
				// - in this case the source location
				sourceEnrichment := &schema.SourceEnrichment{
					CommonFields: schema.CommonFields{
						TpSourceType:     artifact_source_config.FileSystemSourceIdentifier,
						TpSourceLocation: &path,
					},
				}

				artifactInfo := &types.ArtifactInfo{Name: path, SourceEnrichment: sourceEnrichment}
				// notify observers of the discovered artifact
				return s.OnArtifactDiscovered(ctx, artifactInfo)
			}
			return nil
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

func (s *FileSystemSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {

	// TODO consider large/remote files/download progress https://github.com/turbot/tailpipe-plugin-sdk/issues/10
	//s.NotifyObservers(events.NewArtifactDownloadProgress(request, info))

	// notify observers of the discovered artifact
	// NOTE: for now just pass on the info as is
	// if the file was downloaded we would update the Name to the local path, leaving OriginalName as the source path
	// TODO CREATE collection state data https://github.com/turbot/tailpipe-plugin-sdk/issues/11
	return s.OnArtifactDownloaded(ctx, info)
}
