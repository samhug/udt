package udt

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
)

// NewResults reads XML data and constructs a Results object from it
func NewResults(xmlResults io.ReadCloser) *Results {

	return &Results{
		r:       xmlResults,
		decoder: xml.NewDecoder(xmlResults),
		err:     nil,
	}
}

// Results parses XML results into json style objects
type Results struct {
	r              io.ReadCloser
	decoder        *xml.Decoder
	err            error
	inRootElem     bool
	recordElemName string
}

// ReadRecord parses the XML response stream and returns the next record
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
			return nil, r.err
		}
		if t == nil {
			r.err = err
			return nil, r.err
		}

		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			e, _ := t.(xml.StartElement)

			if se.Name.Local == "ROOT" {
				r.inRootElem = true
				continue
			} else if r.inRootElem && r.recordElemName == "" {
				r.recordElemName = se.Name.Local
			}

			if r.inRootElem && se.Name.Local == r.recordElemName {
				inRecordElem = true

				// Add the element attributes to the record map
				for _, attr := range e.Attr {
					record[attr.Name.Local] = attr.Value
				}
			} else if inRecordElem && !inMVElem {
				// We're expecting an element name ending in "_MV"
				if se.Name.Local[len(se.Name.Local)-3:] != "_MV" {
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

				/*
					attrName := se.Name.Local[:len(se.Name.Local)-3]
					if _, ok := record[attrName]; !ok {
						record[attrName] = []string{}
					}

					if len(e.Attr) < 1 {
						continue
					}
					attr := e.Attr[0]
					record[attrName] = append(record[attrName].([]string), attr.Value)
				*/
			} else if inRecordElem && inMVElem {
				// We're expecting an element name ending in "_MS"
				if se.Name.Local[len(se.Name.Local)-3:] != "_MS" {
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
	return r.r.Close()
}
