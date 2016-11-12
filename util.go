package goparquet

import (
	"fmt"
	"github.com/zenixls2/goparquet/thrift"
)

// Convert Thrift enums to / from parquet enums

func (tp *Type) ToThrift() thrift.Type {
	return thrift.Type(*tp)
}

func (tp *LogicalType) ToThrift() thrift.ConvertedType {
	// item 0 is NONE
	if *tp == LogicalType_NONE {
		panic(fmt.Errorf("LogicalType::NONE cannot be convert back to thrift"))
	}
	return thrift.ConvertedType(*tp - 1)
}

func (tp *Repetition) ToThrift() thrift.FieldRepetitionType {
	return thrift.FieldRepetitionType(*tp)
}

func (tp *Encoding) ToThrift() thrift.Encoding {
	return thrift.Encoding(*tp)
}

func (tp *Compression) ToThrift() thrift.CompressionCodec {
	return thrift.CompressionCodec(*tp)
}
