package parquet

type LogicalType int

const (
	LogicalType_NONE             LogicalType = 0
	LogicalType_UTF8             LogicalType = 1
	LogicalType_MAP              LogicalType = 2
	LogicalType_MAP_KEY_VALUE    LogicalType = 3
	LogicalType_LIST             LogicalType = 4
	LogicalType_ENUM             LogicalType = 5
	LogicalType_DECIMAL          LogicalType = 6
	LogicalType_DATE             LogicalType = 7
	LogicalType_TIME_MILLIS      LogicalType = 8
	LogicalType_TIME_MICROS      LogicalType = 9
	LogicalType_TIMESTAMP_MILLIS LogicalType = 10
	LogicalType_TIMESTAMP_MICROS LogicalType = 11
	LogicalType_UINT_8           LogicalType = 12
	LogicalType_UINT_16          LogicalType = 13
	LogicalType_UINT_32          LogicalType = 14
	LogicalType_UINT_64          LogicalType = 15
	LogicalType_INT_8            LogicalType = 16
	LogicalType_INT_16           LogicalType = 17
	LogicalType_INT_32           LogicalType = 18
	LogicalType_INT_64           LogicalType = 19
	LogicalType_JSON             LogicalType = 20
	LogicalType_BSON             LogicalType = 21
	LogicalType_INTERVAL         LogicalType = 22
)

type Repetition int

const (
	Repetition_REQUITED Repetition = 0
	Repetition_OPTIONAL Repetition = 1
	Repetition_REPEATED Repetition = 2
)

type Compression int

const (
	Compression_UNCOMPRESSED Compression = 0
	Compression_SNAPPY       Compression = 1
	Compression_GZIP         Compression = 2
	Compression_LZO          Compression = 3
	Compression_BROTLI       Compression = 4
)
