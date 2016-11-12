package file

import (
	"bytes"
	"github.com/zenixls2/goparquet"
	"github.com/zenixls2/goparquet/column"
	"github.com/zenixls2/goparquet/schema"
	"github.com/zenixls2/goparquet/thrift"
	"io"
	"unsafe"
)

var PARQUET_MAGIC = []byte{'P', 'A', 'R', '1'}

type SerializedPageWriter struct {
	PageWriter
	Sink                  io.Writer
	Metadata              *ColumnChunkMetaDataBuilder
	NumValues             int64
	DictionaryPageOffset  int64
	DataPageOffset        int64
	TotalUncompressedSize int64
	TotalCompressedSize   int64
	Compressor            *Codec
}

func (s *SerializedPageWriter) WriteDataPage(page *column.CompressedDataPage) int64 {
	uncompressed_size := page.UncompressedSize()
	compressed_data := page.Buffer()
	data_page_header := schema.DataPageHeader{
		NumValues:               page.NumValues(),
		Encoding:                page.Encoding().ToThrift(),
		DefinitionLevelEncoding: page.DefinitionLevelEncoding().ToThrift(),
		RepetitionLevelEncoding: page.RepetitionLevelEncoding().ToThrift(),
		Statistics:              page.Statistics().ToThrift(),
	}
	page_header := schema.PageHeader{
		Type:                 goparquet.PageType_DATA_PAGE,
		UncompressedPageSize: uncompressed_size,
		CompressedPageSize:   compressed_data.Len(),
		DataPageHeader:       data_page_header,
	}
	// TODO(PARQUET-594) crc checksum

	start_pos := s.Sink.(*bytes.Buffer).Len()
	if s.DataPageOffset == 0 {
		s.DataPageOffset = start_pos
	}
	thrift.SerializeThrfitMsg(&page_header, unsafe.Sizeof(page_header), s.Sink)
	header_size := s.Sink.(*bytes.Buffer).Len() - start_pos
	s.Sink.Write(compressed_data.Bytes())
	s.TotalUncompressedSize += uncompressed_size + header_size
	s.TotalCompressedSize += compressed_data.Len() + header_size
	s.NumValues += page.NumValues()

	return s.Sink.(*bytes.Buffer).Len() - start_pos
}

func (s *SerializedPageWriter) WriteDictionaryPage(page *DictionaryPage) int64 {
	uncompressed_size := page.Size()
	compressed_data := s.Compress(page.Buffer())
	dict_page_header := schema.DictionaryPageHeader{
		NumValues: page.NumValues(),
		Encoding:  page.Encoding().ToThrfit(),
		IsSorted:  page.IsSorted(),
	}
	page_header := schema.PageHeader{
		Type:                 goparquet.PageType_DICTIONARY_PAGE,
		UncompressedPageSize: uncompressed_size,
		CompressedPageSize:   compressed_data.Len(),
		DictionaryPageHeader: dict_page_header,
	}
	// TODO(PARQUET-594) crc checksum

	start_pos := s.Sink.(*bytes.Buffer).Len()
	if s.DictionaryPageOffset == 0 {
		s.DictionaryPageOffset = start_pos
	}
	thrift.SerializeThrfitMsg(&page_header, unsafe.Sizeof(page_header), s.Sink)
	header_size := s.Sink.(*bytes.Buffer).Len() - start_pos
	s.Sink.Write(compressed_data.Bytes())
	s.TotalUncompressedSize += uncompressed_size + header_size
	s.TotalCompressedSize += compressed_data.Len() + header_size

	return s.Sink.(*bytes.Buffer).Len() - start_pos
}

func (s *SerializedPageWriter) Compress(buffer *bytes.Buffer) *bytes.Buffer {
	// Fast path, no compressor available
	if s.Compressor == nil {
		return buffer
	}

	// Compress the data
	return s.Compressor.Compress(buffer)
}

func (s *SerializedPageWriter) Close(has_dictionary bool, fallback bool) {
	// index_page_offset = 0 since they are not supported
	// TODO: Remove default fallback = 'false' when implemented
	s.Metadata.Finish(s.NumValues, s.DictionaryPageOffset, 0, s.DataPageOffset,
		s.TotalCompressedSize, s.TotalUncompressedSize, has_dictionary, fallback)
}

func NewSerializedPageWriter(sink io.Writer, codec *Compression, metadata *ColumnChunkMetaDataBuilder, allocator *MemoryAllocator) *SerializedPageWriter {
	return &SerializedPageWriter{
		Sink:                  sink,
		Metadata:              metadata,
		NumValues:             0,
		DictionaryPageOffset:  0,
		DataPageOffset:        0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		Compressor:            NewCodec(codec),
	}
}

func (e *EncodedStatistics) ToThrift() schema.Statistics {
	statistics := new(schema.Statistics)
	if e.HasMin {
		statistics.SetMin(e.Min())
	}
	if e.HasMax {
		statistics.SetMax(e.Max())
	}
	if e.HasNullCount {
		statistics.SetNullCount(e.NullCount)
	}
	if e.HasDistinctCount {
		statistics.SetDistinctCount(e.DistinctCount)
	}
	return statistics
}

// -----------------------------------------------------------------
// RowGroupSerializer

type RowGroupSerializer struct {
	RowGroupWriterContents
	NumRows             int
	Sink                io.Writer
	Metadata            *RowGroupMetaDataBuilder
	Properties          *WriterProperties
	TotalBytesWritten   int64
	Closed              bool
	CurrentColumnWriter *ColumnWriter
}

func (r *RowGroupSerializer) NumColumns() int {
	return r.Metadata.NumColumns()
}

func (r *RowGroupSerializer) NumRows() int64 {
	return r.NumRows
}

func (r *RowGroupSerializer) NextColumn() *ColumnWriter {
	// Throws an error if more columns are being written
	col_meta := r.Metadata.NextColumnChunnk()
	if r.CurrentColumnWriter != nil {
		r.TotalBytesWritten += r.CurrentColumnWriter.Close()
	}
	column_descr := col_meta.Descr()
	pager := NewPageWriter(NewSerializedPageWriter(
		r.Sink, r.Properties.Compression(column_descr.Path()),
		col_meta, r.Properties.Allocator()))
	r.CurrentColumnWriter = NewColumnWriterMake(col_meta, pager, r.NumRows,
		r.Properties)
	return r.CurrentColumnWriter
}

func (r *RowGroupSerializer) Close() {
	if !r.Closed {
		r.Closed = true
		if r.CurrentColumnWriter != nil {
			r.TotalBytesWritten += r.CurrentColumnWriter.Close()
			r.CurrentColumnWriter = nil
		}
		// Ensure all columns have been written
		r.Metadata.Finish(r.TotalBytesWritten)
	}
}

func NewRowGroupSerializer(num_rows int64, sink io.Writer, metadata *RowGroupMetaDataBuilder, properties *WriterProperties) *RowGroupSerializer {
	return &RowGroupSerializer{
		NumRows:           num_rows,
		Sink:              sink,
		Metadata:          metadata,
		Properties:        properties,
		TotalBytesWritten: 0,
		Closed:            false,
	}
}

// -----------------------------------------------------------------
// FileSerializer

type FileSerializer struct {
	ParquetFileWriterContents
	Sink           io.Writer
	IsOpen         bool
	Properties     *WriterProperties
	NumRowGroups   int
	NumRows        int64
	Metadata       *FileMetaDataBuilder
	RowGroupWriter *RowGroupWriter
}

func (f *FileSerializer) Close() {
	if f.IsOpen {
		if f.RowGroupWriter != nil {
			f.RowGroupWriter.Close()
			f.RowGroupWriter = nil
		}

		// Write magic bytes and metadata
		f.WriteMetaData()
		f.Sink.Close()
		f.IsOpen = false
	}
}

func (f *FileSerializer) AppendRowGroup(num_rows int64) *RowGroupWriter {
	if f.RowGroupWriter != nil {
		f.RowGroupWriter.Close()
	}
	f.NumRows += num_rows
	f.NumRowGroups++
	rg_metadata := r.Metadata.AppendRowGroup(num_rows)
	var contents RowGroupWriterContents
	contents = NewRowGroupSerializer(num_rows, f.Sink, rg_metadata, f.Properties)
	f.RowGroupWriter = NewRowGroupWriter(contents)
	return f.RowGroupWriter
}

func (f *FileSerializer) Properties() *WriterProperties {
	return f.Properties
}

func (f *FileSerializer) NumColumns() int {
	return f.Schema.NumColumns()
}

func (f *FileSerializer) NumRowGroups() int {
	return f.NumRowGroups
}

func (f *FileSerializer) NumRows() int64 {
	return f.NumRows
}

func (f *FileSerializer) StartFile() {
}

func (f *FileSerializer) WriteMetaData() {
	// Write MetaData
	metadata_len := f.Sink.(*bytes.Buffer).Len()

	// Get a FileMetaData
	metadata := f.Metadata.Finish()
	metadata.WriteTo(f.Sink)
	metadata_len = f.Sink.(*bytes.Buffer).Len()

	// Write Footer
	f.Sink.Write(//HERE)
}

func NewFileSerializerOpen(sink io.Writer, schema *schema.GroupNode, properties *WriterProperties) ParquetFileWriterContents {
}
