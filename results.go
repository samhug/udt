package udt

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/text/encoding/charmap"
)

// NewResults reads XML data and constructs a Results object from it
func NewResults(xmlResults io.ReadCloser) *Results {

	r := charmap.ISO8859_1.NewDecoder().Reader(xmlResults)

	d := xml.NewDecoder(r)

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

type xmlDataElement struct {
	Data string `xml:",chardata"`
}

// ReadRecord parses the XML response stream and returns the next record
// The XML response stream is expected to be of the format
// <?xml version="1.0"?>
// <ROOT>
// <RECORD>
//   <_ID_MV>
//     <_ID_MS>
//       <_ID>00001</_ID>
//     </_ID_MS>
//   </_ID_MV>
//   <FIELD1>VALUE</FIELD1>
// </RECORD>
// </ROOT>
func (r *Results) ReadRecord() (map[string]interface{}, error) {

	if r.err != nil {
		return nil, r.err
	}

	record := make(map[string]interface{})
	inRecordElem := false
	inMVElem := false
	inMSElem := false
	mvElemName := ""
	msElemName := ""

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

			// if we're in the ROOT element and we don't know the record element name yet
			// this must be the record element name so we'll save it's name.
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

			// Is this a top-level FIELD element
			if inRecordElem && !inMVElem && !strings.HasSuffix(se.Name.Local, "_MV") {
				var dataEl xmlDataElement
				if err := r.decoder.DecodeElement(&dataEl, &e); err != nil {
					r.err = fmt.Errorf("failed to read element '%s': %w", se.Name.Local, err)
					return nil, r.err
				}

				record[se.Name.Local] = dataEl.Data
				continue
			}

			// Is this a FIELD_MV element
			if inRecordElem && !inMVElem && strings.HasSuffix(se.Name.Local, "_MV") {
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

			// Is this a FIELD element in an MV element
			if inRecordElem && inMVElem && !inMSElem && !strings.HasSuffix(se.Name.Local, "_MS") {
				var dataEl xmlDataElement
				if err := r.decoder.DecodeElement(&dataEl, &e); err != nil {
					r.err = fmt.Errorf("failed to read element '%s': %w", se.Name.Local, err)
					return nil, r.err
				}

				lmv := len(record[mvElemName].([]map[string]interface{}))

				record[mvElemName].([]map[string]interface{})[lmv-1][se.Name.Local] = dataEl.Data
				continue
			}

			// Is this a FIELD_MS element
			if inRecordElem && inMVElem && !inMSElem && strings.HasSuffix(se.Name.Local, "_MS") {
				inMSElem = true
				msElemName = se.Name.Local

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

			// Is this a FIELD element in an MS element
			if inRecordElem && inMSElem {
				var dataEl xmlDataElement
				if err := r.decoder.DecodeElement(&dataEl, &e); err != nil {
					r.err = fmt.Errorf("failed to read element '%s': %w", se.Name.Local, err)
					return nil, r.err
				}

				lmv := len(record[mvElemName].([]map[string]interface{}))
				lms := len(record[mvElemName].([]map[string]interface{})[lmv-1][msElemName].([]map[string]string))

				record[mvElemName].([]map[string]interface{})[lmv-1][msElemName].([]map[string]string)[lms-1][se.Name.Local] = dataEl.Data
				continue
			}
		case xml.EndElement:
			if inRecordElem && !inMVElem && se.Name.Local == r.recordElemName { // Exit record element
				inRecordElem = false
				recordComplete = true
			} else if inMVElem && !inMSElem && se.Name.Local == mvElemName { // Exit MV Element
				inMVElem = false
				mvElemName = ""
			} else if inMSElem && se.Name.Local == msElemName { // Exit MS Element
				inMSElem = false
				msElemName = ""
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
