package udt

// RecordReader provides an interface for reading result records one at a time
type RecordReader interface {
	ReadRecord() (map[string]interface{}, error)
	Close() error
}
