package udt

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
)

func NewResults(xmlResults io.ReadCloser) *Results {

	return &Results{
		r:       xmlResults,
		decoder: xml.NewDecoder(xmlResults),
		err:     nil,
	}
}

type Results struct {
	r              io.ReadCloser
	decoder        *xml.Decoder
	err            error
	inRootElem     bool
	recordElemName string
}

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
				// We're expecting an element of type NAME_MV
				if se.Name.Local[len(se.Name.Local)-3:] != "_MV" {
					r.err = fmt.Errorf("expected an element with a name ending in '_MV' got '%s' instead", se.Name.Local)
					return nil, r.err
				}
				inMVElem = true
				mvElemName = se.Name.Local

				attrName := se.Name.Local[:len(se.Name.Local)-3]
				if _, ok := record[attrName]; !ok {
					record[attrName] = []string{}
				}

				if len(e.Attr) < 1 {
					continue
				}
				attr := e.Attr[0]
				record[attrName] = append(record[attrName].([]string), attr.Value)
			} else if inRecordElem && inMVElem {
				// We're expecting an element of type NAME_MS
				if se.Name.Local == mvElemName[len(mvElemName)-3:]+"_MS" {
					r.err = fmt.Errorf("expected an element with a name ending in '_MS' got '%s' instead", se.Name.Local)
					return nil, r.err
				}

				attrName := se.Name.Local[:len(se.Name.Local)-3]
				if len(e.Attr) < 1 {
					r.err = fmt.Errorf("expected element to have 1 attribute named '%s'", attrName)
					return nil, r.err
				}
				attr := e.Attr[0]
				record[attrName] = append(record[attrName].([]string), attr.Value)
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

func (r *Results) Close() error {
	r.err = errors.New("Results has already been closed")
	return r.r.Close()
}
