package sources

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
)

const (
	FileSourceIdentifier = "file"
)

type FileSourceConfig struct {
	artifact_source_config.ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Paths []string `hcl:"paths"`
}

func (f *FileSourceConfig) Validate() error {
	// validate the base fields
	if err := f.ArtifactSourceConfigBase.Validate(); err != nil {
		return err
	}

	// validate we have at least one path
	if len(f.Paths) == 0 {
		return fmt.Errorf("required field: paths can not be empty")
	}

	// validate paths exist on the file system
	for _, path := range f.Paths {
		if !utils.IsValidDir(path) {
			return fmt.Errorf("path %s is not a directory or does not exist", path)
		}
	}

	return nil
}

func (f *FileSourceConfig) Identifier() string {
	return FileSourceIdentifier
}
