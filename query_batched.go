package udt

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/go-uuid"
)

// QueryConfig represents a query to be run against a Unidata database
type QueryConfig struct {
	SelectStmt string
	File       string
	Fields     []string
	BatchSize  int
}

const defaultBatchSize = 10000

// NewQueryBatched returns a queryBatched object that implements the RecordReader interface
func NewQueryBatched(client *Client, query *QueryConfig) (*QueryBatched, error) {

	// If we're not provided a BatchSize, use the default
	if query.BatchSize <= 0 {
		query.BatchSize = defaultBatchSize
	}

	q := &QueryBatched{
		client: client,
		query:  query,
	}

	if err := q.doSelect(); err != nil {
		return nil, err
	}

	return q, nil
}

// QueryBatched represents a batched query operation
type QueryBatched struct {
	client *Client
	query  *QueryConfig

	err          error
	queryUUID    string
	recordCount  int
	batchCursor  int
	batchRecords RecordReader
}

const udtProgFile = "BP"

const selectSrcTmpl = `
$BASICTYPE "U"
EXECUTE %s
EXECUTE %s
`

func (q *QueryBatched) doSelect() (err error) {

	q.queryUUID, err = uuid.GenerateUUID()
	if err != nil {
		return
	}

	progSrc := fmt.Sprintf(selectSrcTmpl,
		QuoteString(q.query.SelectStmt),
		QuoteString(fmt.Sprintf("SAVE.LIST %s", QuoteString(q.queryUUID))),
	)
	progName := "SELECT-" + q.queryUUID

	if err = q.client.CompileBasicProgram(udtProgFile, progName, progSrc); err != nil {
		return
	}

	r, err := q.client.ExecutePhantom(fmt.Sprintf("RUN %s '%s'", udtProgFile, progName))
	if err != nil {
		return
	}
	defer safeClose(r, "failed to close BASIC program response", &err)

	if err = q.client.DeleteBasicProgram(udtProgFile, progName); err != nil {
		return
	}

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	// Expecting output of the form: "\n10 records selected to list 0.\n\n10 key(s) saved to 1 record(s).\n"
	re := regexp.MustCompile(`\n(\d+) key\(s\) saved to \d+ record\(s\).\n`)
	matches := re.FindSubmatch(buf)
	if matches == nil {
		err = fmt.Errorf("unexpected response when selecting records: %s", buf)
		return
	}

	q.recordCount, err = strconv.Atoi(string(matches[1]))
	if err != nil {
		panic("doSelect: error parsing int")
	}

	return
}

const batchSrcTmpl = `
$BASICTYPE "U"

** The name of the saved list to load record ids from
LISTNAME = %s

** File name to retrieve records from
FILENAME = %s

** Fields to list
FILEFIELDS = %s

** The index of the first record of the batch
FIRSTI = %d

** The maximum number of records to include in a batch
BATCH = %d

** =====

** Load the saved list from file into select list 0
GETLIST LISTNAME SETTING TCOUNT ELSE
	PRINT "Failed to load select list ":LISTNAME
	STOP
END

*PRINT TIMEDATE():" : Done, loaded ":TCOUNT:" records."

*PRINT TIMEDATE():" : Build a dynamic array with only the record ids for this batch ..."

** The index of the last record of the batch
LASTI = FIRSTI + BATCH - 1

RECORDS = ""

I=0
LOOP WHILE I <= LASTI DO

	** Read the next record id from select list 0
	READNEXT RECORD.ID ELSE
		PRINT "Failed to READNEXT for record I=":I
		EXIT
	END

	** If this record id is before the start of our batch, skip it
	IF I < FIRSTI THEN
		I += 1
		CONTINUE
	END

	RECORDS = INSERT(RECORDS, -1, 0, 0, RECORD.ID)
	I += 1
REPEAT

FORMLIST RECORDS TO 0

LISTSTMT = "LIST ":FILENAME:" ":FILEFIELDS:" TOXML"

EXECUTE LISTSTMT
`

func (q *QueryBatched) getNextBatch() error {

	batchSize := q.query.BatchSize
	if q.recordCount-q.batchCursor < batchSize {
		batchSize = q.recordCount - q.batchCursor
	}

	progSrc := fmt.Sprintf(batchSrcTmpl,
		QuoteString(q.queryUUID),
		QuoteString(q.query.File),
		QuoteString(strings.Join(q.query.Fields, " ")),
		q.batchCursor,
		batchSize,
	)
	progName := "LIST-" + q.queryUUID

	if err := q.client.CompileBasicProgram(udtProgFile, progName, progSrc); err != nil {
		return err
	}

	r, err := q.client.ExecutePhantom(fmt.Sprintf("RUN %s '%s'", udtProgFile, progName))
	if err != nil {
		return err
	}

	if err := q.client.DeleteBasicProgram(udtProgFile, progName); err != nil {
		return err
	}

	q.batchRecords = NewResults(r)

	q.batchCursor += batchSize
	return nil
}

// ReadRecord implements the RecordReader interface
func (q *QueryBatched) ReadRecord() (map[string]interface{}, error) {

	if q.err != nil {
		return nil, q.err
	}

	// If we don't have a batch to read from, get one
	if q.batchRecords == nil {
		if err := q.getNextBatch(); err != nil {
			return nil, fmt.Errorf("failed to fetch first batch of records: %s", err)
		}
	}

	record, err := q.batchRecords.ReadRecord()

	// If we've reached the end of this batch but we're not on the last batch
	if err == io.EOF && q.batchCursor < q.recordCount {
		if err := q.batchRecords.Close(); err != nil {
			return nil, fmt.Errorf("failed to close record batch reader: %s", err)
		}

		if err := q.getNextBatch(); err != nil {
			return nil, fmt.Errorf("failed to fetch record batch [%d-%d]: %s", q.batchCursor, q.batchCursor+q.query.BatchSize, err)
		}

		return q.batchRecords.ReadRecord()
	}

	return record, err
}

// Count returns the number of records that were selected
func (q *QueryBatched) Count() int {
	return q.recordCount
}

// Close closes the RecordReader
func (q *QueryBatched) Close() error {

	if q.err != nil {
		return q.err
	}

	q.err = errors.New("record reader has already been closed")

	if q.batchRecords != nil {
		if err := q.batchRecords.Close(); err != nil {
			return err
		}
	}

	if err := q.client.SavedListDelete(q.queryUUID); err != nil {
		return err
	}

	return nil
}
