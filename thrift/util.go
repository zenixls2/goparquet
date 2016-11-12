package thrift

import (
	"bytes"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/zenixls2/goparquet"
	"io"
)

type T interface {
	Write(oprot thrift.TProtocol) error
	Read(iprot thrift.TProtocol) error
}

// Convert Thrift enums to / from parquet enums

func (tp *Type) FromTrift() goparquet.Type {
	return goparquet.Type(*tp)
}

func (tp *ConvertedType) FromThrift() goparquet.LogicalType {
	// item 0 is NONE
	return goparquet.LogicalType(*tp + 1)
}

func (tp *FieldRepetitionType) FromThrift() goparquet.Repetition {
	return goparquet.Repetition(*tp)
}

func (tp *Encoding) FromThrift() goparquet.Encoding {
	return goparquet.Encoding(*tp)
}

func (tp *CompressionCodec) FromThrift() goparquet.Compression {
	return goparquet.Compression(*tp)
}

// Thrift struct serialization / deserialization utilities

/*
 * Deserialize a thrift message from buf/len. buf/len must at least contain
 * all the bytes needed to store the thrift message. On return, len will be
 * set to the actual length of the header.
 */
func DeserializeThriftMsg(buf []byte, length int, deserialized_msg T) uint64 {
	// limit to thrift-go's flexibility
	tmem_transport := thrift.NewTMemoryBufferLen(length)
	tmem_transport.Buffer = bytes.NewBuffer(buf)
	tproto_factory := thrift.NewTCompactProtocolFactory()
	tproto := tproto_factory.GetProtocol(tmem_transport)
	if err := deserialized_msg.Read(tproto); err != nil {
		panic(fmt.Errorf("Couldn't deserialize thrift: %v\n", err))
	}
	return tmem_transport.RemainingBytes()
}

/*
 * Serialize obj into a buffer. The result is returned as a string.
 * The arguments are the object to be serialized and
 * the expected size of the serialized object
 */
func SerializeTriftMsg(obj T, length int, out io.Writer) {
	mem_buffer := thrift.NewTMemoryBufferLen(length)
	tproto_factory := thrift.NewTCompactProtocolFactory()
	tproto := tproto_factory.GetProtocol(mem_buffer)
	mem_buffer.Buffer.Reset()
	if err := obj.Write(tproto); err != nil {
		panic(fmt.Errorf("Couldn't serialize thrift: %v\n", err))
	}
	out_buffer := mem_buffer.Buffer.Bytes()
	out.Write(out_buffer)
}
