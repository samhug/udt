package udt

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/template"

	"github.com/hashicorp/go-uuid"
)

// QueryConfig represents a query to be run against a Unidata database
type QueryConfig struct {
	Select    []string
	File      string
	Fields    []string
	BatchSize int
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

	if err := q.run(); err != nil {
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
	udtProgName  string
	udtProc      *UdtProc
	procScanner  *bufio.Scanner
	recordCount  int
	batchCursor  int
	batchRecords RecordReader
}

const udtProgFile = "BP"
const udtProgSrcTmpl = `
$BASICTYPE "U"

** Should be a list of statements that will populate select list 0
SELECTSCRIPT = ''
{{range .SelectScript}}
SELECTSCRIPT = INSERT(SELECTSCRIPT, -1, 0, 0, {{.}})
{{end}}

** File from which to list records
** Ex: 'CUSTOMER'
LISTFILE = {{.ListFile}}

** Space delimited list of fields to retrieve
** Ex: 'NAME CITY'
FILEFIELDS = {{.FileFields}}

** Unique id to prefix output file names
** Ex: '730ba7a4-f267-11e8-8eb2-f2801f1b9fd1'
QUERYID = {{.QueryId}}

** Number of records to include in each result batch
** Ex: 10000
BATCHSIZE = {{.BatchSize}}

** Set DEBUG=1 to output debug messages
DEBUG = {{.Debug}}

** ======

CURSOR = 0

IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|will run (':SELECTSCRIPT:') and retrieve results from (':LISTFILE:') in batches of (':BATCHSIZE:')'

IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|select records'
GOSUB DOSELECT
IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|selected ':RECORDCOUNT:' records'
PRINT '|SELECTED|':RECORDCOUNT

BATCHI = 0
LOOP WHILE BATCHI < RECORDCOUNT/BATCHSIZE DO
  IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|build select list for batch ':BATCHI

  GOSUB DOGETNEXTBATCH

  IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|list batch ':BATCHI

  GOSUB DOLIST

  BATCHI += 1
REPEAT

IF DEBUG=1 THEN PRINT '|DEBUG|':SYSTEM(12):'|done'

PRINT '|DONE'

GOSUB EXIT

** ======

DOSELECT:
  EXECUTE SELECTSCRIPT
  RECORDCOUNT = SYSTEM(11)
  EXECUTE 'SAVE.LIST'
  EXECUTE 'GET.LIST TO 1'
  EXECUTE 'DELETE.LIST'
  RETURN

DOGETNEXTBATCH:
  RECORDIDS = ''

  ** The index of the last record of the batch
  LASTI = CURSOR + BATCHSIZE - 1
  IF LASTI > RECORDCOUNT-1 THEN LASTI = RECORDCOUNT-1

  LOOP WHILE CURSOR <= LASTI DO

    ** Read the next record id from select list 1
    READNEXT RECORD.ID FROM 1 ELSE
      PRINT '|ERROR|failed to READNEXT for record number ':CURSOR
      EXIT
    END

    RECORDIDS = INSERT(RECORDIDS, -1, 0, 0, RECORD.ID)
    CURSOR += 1
  REPEAT

  FORMLIST RECORDIDS TO 0

  RETURN

DOLIST:
  OUTFILENAME = QUERYID:'_':BATCHI
  LISTSTMT = 'LIST ':LISTFILE:' ':FILEFIELDS:' TOXML TO ':OUTFILENAME
  EXECUTE LISTSTMT
  PRINT '|RESULTBATCH|':BATCHI:'|_XML_/':OUTFILENAME:'.xml'
  RETURN

EXIT:
`

// Tprintf passed template string is formatted usign its operands and returns the resulting string.
// Spaces are added between operands when neither is a string.
// From: https://forum.golangbridge.org/t/named-string-formatting/3802/5
func tprintf(tmpl string, data map[string]interface{}) string {
	t := template.Must(template.New("udtsrc").Parse(tmpl))
	buf := &bytes.Buffer{}
	if err := t.Execute(buf, data); err != nil {
		panic(fmt.Sprintf("Failed to render UDT source template: %s", err))
	}
	return buf.String()
}

func (q *QueryBatched) run() (err error) {

	q.queryUUID, err = uuid.GenerateUUID()
	if err != nil {
		return
	}

	quotedScript := make([]string, len(q.query.Select))
	for i := 0; i < len(q.query.Select); i++ {
		quotedScript[i] = QuoteString(q.query.Select[i])
	}

	q.udtProgName = "ETL-" + q.queryUUID
	progSrc := tprintf(udtProgSrcTmpl, map[string]interface{}{
		"SelectScript": quotedScript,
		"ListFile":     QuoteString(q.query.File),
		"FileFields":   QuoteString(strings.Join(q.query.Fields, " ")),
		"QueryId":      QuoteString(q.queryUUID),
		"BatchSize":    q.query.BatchSize,
		"Debug":        1,
	})

	if err = q.client.CompileBasicProgram(udtProgFile, q.udtProgName, progSrc); err != nil {
		return
	}

	// The -N option disables output paging and is required to capture output longer than one screen
	q.udtProc, err = q.client.Execute(fmt.Sprintf("RUN %s %s -N", udtProgFile, q.udtProgName))
	if err != nil {
		return
	}

	q.procScanner = bufio.NewScanner(q.udtProc.Stdout)
loop:
	for q.procScanner.Scan() {
		line := q.procScanner.Text()
		if len(line) == 0 || line[0] != '|' {
			continue
		}

		// Remove the first pipe symbol and split the string
		parts := strings.Split(line[1:], "|")

		switch parts[0] {
		case "SELECTED":
			strCount := string(parts[1])
			q.recordCount, err = strconv.Atoi(strCount)
			if err != nil {
				panic(fmt.Sprintf("Run: error parsing record count from SELECTED message. received (%s)", strCount))
			}
			break loop
			//default:
			//	log.Println("UDT Agent:", line)
		}
	}
	if err := q.procScanner.Err(); err != nil {
		return err
	}

	return
}

func (q *QueryBatched) getNextBatch() error {

	batchSize := q.query.BatchSize
	if q.recordCount-q.batchCursor < batchSize {
		batchSize = q.recordCount - q.batchCursor
	}

loop:
	for q.procScanner.Scan() {
		line := q.procScanner.Text()
		if len(line) == 0 || line[0] != '|' {
			continue
		}

		// Remove the first pipe symbol and split the string
		parts := strings.Split(line[1:], "|")

		switch parts[0] {
		case "RESULTBATCH":
			//log.Println("UDT Agent:", line)
			path := parts[2]
			// Assert the provided path is in the _XML_ sub-directory so we avoid accidentally
			// deleting something important
			if !strings.HasPrefix(parts[2], "_XML_/") {
				panic(fmt.Sprintf("Unexpected file location given: %s", path))
			}

			f, err := q.client.RetrieveAndDeleteFile(path)
			if err != nil {
				panic(fmt.Sprintf("Failed to retrieve file contents: %s", err))
			}

			q.batchRecords = NewResults(f)
			q.batchCursor += batchSize
			break loop
			//default:
			//	log.Println("UDT Agent:", line)
		}
	}
	if err := q.procScanner.Err(); err != nil {
		return err
	}

	if q.batchRecords == nil {
		return fmt.Errorf("getNextBatch: expected to receive RESULTBATCH message but never did.")
	}

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

	if err := q.client.DeleteBasicProgram(udtProgFile, q.udtProgName); err != nil {
		return err
	}

	return nil
}
