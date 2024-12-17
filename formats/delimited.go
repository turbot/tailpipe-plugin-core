package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"strings"
)

type Delimited struct {
	// Option to skip type detection for CSV parsing and assume all columns to be of type VARCHAR
	AllVarchar *bool `hcl:"all_varchar"`

	// Option to allow the conversion of quoted values to NULL values
	AllowQuotedNulls *bool `hcl:"allow_quoted_nulls"`

	// Specifies the date format to use when parsing dates.
	//DateFormat *string `hcl:"date_format"`

	// The decimal separator of numbers.
	DecimalSeparator *string `hcl:"decimal_separator"`

	// 	Specifies the delimiter character that separates columns within each row (line) of the file.
	Delimiter *string `hcl:"delimiter"`

	// Specifies the string that should appear before a data character sequence that matches the quote value.
	Escape *string `hcl:"escape"`

	// Whether or not an extra filename column should be included in the result.
	Filename *bool `hcl:"filename"`

	// Do not match the specified columns' values against the NULL string.
	// In the default case where the NULL string is empty,
	//this means that empty values will be read as zero-length strings rather than NULLs.
	ForceNotNull *[]string `hcl:"force_not_null"`

	// Specifies that the file contains a header line with the names of each column in the file.
	Header *bool `hcl:"header"`

	// Option to ignore any parsing errors encountered – and instead ignore rows with errors.
	IgnoreErrors *bool `hcl:"ignore_errors"`

	//The maximum line size in bytes.
	MaxLineSize *int `hcl:"max_line_size"`

	// Set the new line character(s) in the file. Options are '\r','\n', or '\r\n'.
	// Note that the CSV parser only distinguishes between single-character and double-character line delimiters.
	// Therefore, it does not differentiate between '\r' and '\n'.
	NewLine *string `hcl:"new_line"`

	// Boolean value that specifies whether or not column names should be normalized,
	// removing any non-alphanumeric characters from them.
	NormalizeNames *bool `hcl:"normalize_names"`

	// If this option is enabled, when a row lacks columns, it will pad the remaining columns on the right with NULL values.
	NullPadding *bool `hcl:"null_padding"`

	//Specifies the string that represents a NULL value or (since v0.10.2) a list of strings that represent a NULL value.
	NullStr *string `hcl:"null_str"`

	//Specifies the quoting string to be used when a data value is quoted.
	Quote *string `hcl:"quote"`

	// The number of sample rows for auto detection of parameters.
	SampleSize *int `hcl:"sample_size"`

	// Specifies the date format to use when parsing timestamps
	TimestampFormat *string `hcl:"timestamp_format"`

	// the roq schema must at the minimum provide mapping for the tp_timestamp field
	Schema *schema.RowSchema `hcl:"schema,block"`
}

func (c *Delimited) Validate() error {
	return nil
}

func (c *Delimited) Identifier() string {
	return constants.SourceFormatDelimited
}

func (c *Delimited) GetSchema() *schema.RowSchema {
	//if c.Schema == nil {
	//	return nil
	//}
	//
	//return c.Schema.ToRowSchema()
	return nil
}

// GetCsvOpts converts the Delimited configuration into a slice of CSV options strings
// in the format expected by DuckDb read_csv function
func (c *Delimited) GetCsvOpts() []string {
	var opts []string

	if c.AllVarchar != nil {
		opts = append(opts, fmt.Sprintf("all_varchar=%v", *c.AllVarchar))
	}

	if c.AllowQuotedNulls != nil {
		opts = append(opts, fmt.Sprintf("allow_quoted_nulls=%v", *c.AllowQuotedNulls))
	}

	if c.DecimalSeparator != nil {
		opts = append(opts, fmt.Sprintf("decimal_separator='%s'", *c.DecimalSeparator))
	}

	if c.Delimiter != nil {
		opts = append(opts, fmt.Sprintf("delimiter='%s'", *c.Delimiter))
	}

	if c.Escape != nil {
		opts = append(opts, fmt.Sprintf("escape='%s'", *c.Escape))
	}

	if c.Filename != nil {
		opts = append(opts, fmt.Sprintf("filename=%v", *c.Filename))
	}

	if c.ForceNotNull != nil && len(*c.ForceNotNull) > 0 {
		forceNotNullValues := strings.Join(*c.ForceNotNull, ",")
		opts = append(opts, fmt.Sprintf("force_not_null=%s", forceNotNullValues))
	}

	if c.Header != nil {
		opts = append(opts, fmt.Sprintf("header=%v", *c.Header))
	}

	if c.IgnoreErrors != nil {
		opts = append(opts, fmt.Sprintf("ignore_errors=%v", *c.IgnoreErrors))
	}

	if c.MaxLineSize != nil {
		opts = append(opts, fmt.Sprintf("max_line_size=%d", *c.MaxLineSize))
	}

	if c.NewLine != nil {
		opts = append(opts, fmt.Sprintf("new_line='%s'", *c.NewLine))
	}

	if c.NormalizeNames != nil {
		opts = append(opts, fmt.Sprintf("normalize_names=%v", *c.NormalizeNames))
	}

	if c.NullPadding != nil {
		opts = append(opts, fmt.Sprintf("null_padding=%v", *c.NullPadding))
	}

	if c.NullStr != nil {
		opts = append(opts, fmt.Sprintf("null_str='%s'", *c.NullStr))
	}

	if c.Quote != nil {
		opts = append(opts, fmt.Sprintf("quote='%s'", *c.Quote))
	}

	if c.SampleSize != nil {
		opts = append(opts, fmt.Sprintf("sample_size=%d", *c.SampleSize))
	}

	if c.TimestampFormat != nil {
		opts = append(opts, fmt.Sprintf("timestamp_format='%s'", *c.TimestampFormat))
	}

	return opts
}
