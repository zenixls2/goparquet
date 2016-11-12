package file

import (
	"io"
)

type RowGroupWriterContents interface {
	NumColumns() int
	NumRows() int64
	NextColumn() *ColumnWriter
	Close()
}

type RowGroupWriter struct {
	Contents RowGroupWriterContents
}

func (r *RowGroupWriter) NextColumn() *ColumnWriter {
	return r.Contents.NextColumn()
}

func (r *RowGroupWriter) Close() {
	if r.Contents != nil {
		r.Contents.Close()
		r.Contents = nil
	}
}

func (r *RowGroupWriter) NumRows() int64 {
}

func NewRowGroupWriter(contents RowGroupWriterContents) *RowGroupWriter {
	return &RowGroupWriter{Contents: contents}
}

type ParquetFileWriterContents interface {
	Close()
	AppendRowGroup(int64) *RowGroupWriter
	NumRows() int64
	NumColumns() int
	NumRowGroups() int
	Properties() *WriterProperties
	Schema() *SchemaDescriptor
}

type ParquetFileWriter struct {
	Contents ParquetFileWriterContents
}

func (p *ParquetFileWriter) Open(contents ParquetFileWriterContents) {
	p.Contents = contents
}

func (p *ParquetFileWriter) Close() {
	if p.Contents != nil {
		p.Contents.Close()
		p.Contents = nil
	}
}

func (p *ParquetFileWriter) AppendRowGroup(num_rows int64) *RowGroupWriter {
	return p.Contents.AppendRowGroup(num_rows)
}

func (p *ParquetFileWriter) NumColumns() int {
}

func (p *ParquetFileWriter) NumRowGroups() int {
}

func (p *ParquetFileWriter) Properties() *WriterProperties {
	return p.Contents.Properties()
}

func (p *ParquetFileWriter) Schema() *SchemaDescriptor {
	return p.Contents.Schema()
}

func (p *ParquetFileWriter) Descr(i int) *ColumnDescriptor {
	return p.Contents.Schema().Column(i)
}

func NewParquetFileWriter() *ParquetFileWriter {
	return new(ParquetFileWriter)
}

func NewParquetFileWriterOpen(sink io.Writer, schema GroupNode,
	properties WriterProperties) *ParquetFileWriter {
	contents := NewFileSerializerOpen(sink, schema, properties)
	result := new(ParquetFileWriter)
	result.Open(contents)
	return result
}
