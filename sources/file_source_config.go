package sources

import (
	"fmt"
	"regexp"

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

	Paths      []string `hcl:"paths"`
	Extensions []string `hcl:"extensions"`
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

	// validate extensions are valid (begin with .)
	if len(f.Extensions) > 0 {
		re := regexp.MustCompile(`^\.[a-zA-Z0-9]+$`)
		for _, ext := range f.Extensions {
			if !re.MatchString(ext) {
				return fmt.Errorf("invalid extension: %s, must begin with a '.' and be suffixed with at least one alphanumeric character", ext)
			}
		}
	}

	return nil
}

func (f FileSourceConfig) Identifier() string {
	return FileSourceIdentifier
}
