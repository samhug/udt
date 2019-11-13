package udt

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

// NewResults reads XML data and constructs a Results object from it
func NewResults(xmlResults io.ReadCloser) *Results {

	//buf := new(bytes.Buffer)
	//buf.ReadFrom(xmlResults)
	//ioutil.WriteFile("./out.xml", buf.Bytes(), 0644)

	//r := ioutils.NewReadCloserWrapper(buf, xmlResults.Close)

	d := xml.NewDecoder(xmlResults)

	return &Results{
		closer:  xmlResults,
		decoder: d,
		err:     nil,
	}
}

// Results parses XML results into json style objects
type Results struct {
	closer         io.Closer
	decoder        *xml.Decoder
	err            error
	inRootElem     bool
	recordElemName string
}

// ReadRecord parses the XML response stream and returns the next record
// The XML response stream is expected to be of the format
// <?xml version="1.0"?>
// <ROOT>
// <RECORD FIELD1 = "VALUE">
//   <_ID_MV>
//     <_ID_MS _ID = "00001"/>
//   </_ID_MV>
// </RECORD>
// </ROOT>
func (r *Results) ReadRecord() (map[string]interface{}, error) {

	if r.err != nil {
		return nil, r.err
	}

	record := make(map[string]interface{})
	inRecordElem := false
	inMVElem := false
	mvElemName := ""

	recordComplete := false

	for !recordComplete {

		// Read tokens from the XML document in a stream.
		t, err := r.decoder.Token()
		if err != nil {
			r.err = err
			if err == io.EOF {
				return nil, err
			}
			return nil, fmt.Errorf("failed to decode XML record: %w", err)
		}

		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			e, _ := t.(xml.StartElement)

			// Is this the ROOT element
			if se.Name.Local == "ROOT" {
				r.inRootElem = true
				continue
			}

			// save the record element name
			if r.inRootElem && r.recordElemName == "" {
				r.recordElemName = se.Name.Local
			}

			// Is this a RECORD element
			if r.inRootElem && se.Name.Local == r.recordElemName {
				inRecordElem = true

				// Add the element attributes to the record map
				for _, attr := range e.Attr {
					record[attr.Name.Local] = attr.Value
				}
				continue
			}

			// Is this a FIELD_MV element
			if inRecordElem && !inMVElem {
				// We're expecting an element name ending in "_MV"
				if !strings.HasSuffix(se.Name.Local, "_MV") {
					r.err = fmt.Errorf("expected an element with a name ending in '_MV', got '%s' instead", se.Name.Local)
					return nil, r.err
				}
				inMVElem = true
				mvElemName = se.Name.Local

				mvRecord := make(map[string]interface{})

				for _, attr := range e.Attr {
					mvRecord[attr.Name.Local] = attr.Value
				}

				// If the record key hasn't been initialized yet, do so
				if _, ok := record[mvElemName]; !ok {
					record[mvElemName] = make([]map[string]interface{}, 0)
				}

				record[mvElemName] = append(record[mvElemName].([]map[string]interface{}), mvRecord)
				continue
			}

			// Is this a FIELD_MS element
			if inRecordElem && inMVElem {
				// We're expecting an element name ending in "_MS"
				if !strings.HasSuffix(se.Name.Local, "_MS") {
					r.err = fmt.Errorf("expected an element with a name ending in '_MS', got '%s' instead", se.Name.Local)
					return nil, r.err
				}
				msElemName := se.Name.Local

				msRecord := make(map[string]string)

				for _, attr := range e.Attr {
					msRecord[attr.Name.Local] = attr.Value
				}

				// If the record key hasn't been initialized yet, do so
				lmv := len(record[mvElemName].([]map[string]interface{}))
				if _, ok := record[mvElemName].([]map[string]interface{})[lmv-1][msElemName]; !ok {
					record[mvElemName].([]map[string]interface{})[lmv-1][msElemName] = make([]map[string]string, 0)
				}

				// TODO: Sorry future me, I'm not proud of this...
				record[mvElemName].([]map[string]interface{})[lmv-1][msElemName] = append(record[mvElemName].([]map[string]interface{})[lmv-1][msElemName].([]map[string]string), msRecord)
				continue
			}
		case xml.EndElement:
			if inRecordElem && se.Name.Local == r.recordElemName { // Exit record element
				inRecordElem = false
				recordComplete = true
			} else if inMVElem && se.Name.Local == mvElemName {
				inMVElem = false
				mvElemName = ""
			}
		}
	}

	return record, nil
}

// Close closes the reader that was passed to us
func (r *Results) Close() error {
	r.err = errors.New("results has already been closed")
	return r.closer.Close()
}
