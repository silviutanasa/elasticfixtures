package elasticfixtures

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
)

type fixtureFile struct {
	path     string
	fileName string
	content  []byte
}

func (f *fixtureFile) fileNameWithoutExtension() (fn string) {
	fn = strings.Replace(f.fileName, filepath.Ext(f.fileName), "", 1)

	return
}

type Loader struct {
	esUrl        string
	fixtureFiles []fixtureFile
}

// New is creating a new loader and loads all the fixture files provided in fileNames variadic
// it parses the file contents and store each file content as an element of type []fixtureFile
func New(serviceUrl string, fileNames ...string) (fixtureLoader *Loader, err error) {
	fixtureLoader = new(Loader)
	fixtureLoader.esUrl = serviceUrl
	fixtureLoader.fixtureFiles, err = fixtureLoader.parseFilesContent(fileNames...)

	return
}

// Clean is deleting all the records from the indexes represented by the fixtures
// it sends a "delete by query" request for every elasticsearch index from the fixtures
// the index name is deducted from the fixture file name without extension
// (ex: for the fixture file example.json, the deducted index name is example)
func (l *Loader) Clean() (err error) {
	cl := http.DefaultClient
	for i := range l.fixtureFiles {
		esIndex := l.fixtureFiles[i].fileNameWithoutExtension()
		reqUrl := fmt.Sprintf("%s/%s/_delete_by_query?conflicts=proceed", l.esUrl, esIndex)
		reqBody := bytes.NewBufferString(`{
							"query": {
							"match_all": {}
							}
							}`)
		delRsp, err := cl.Post(reqUrl, "application/json", reqBody)
		if err != nil {
			return err
		}

		_ = delRsp.Body.Close()
	}

	return nil
}

// Load is saving the fixture data to elasticsearch
// it sends a request for every elasticsearch index
// the index name is deducted from the fixture file name without extension
// (ex: for the fixture file example.json, the deducted index name is example)
func (l *Loader) Load() (err error) {
	cl := http.DefaultClient

	for i := range l.fixtureFiles {
		JSONObjects, err := splitJSONIntoJSONCollection(l.fixtureFiles[i].content)
		if err != nil {
			return fmt.Errorf("esfixtures: invalid data provided for fixture: %v, err: %w", l.fixtureFiles[i].fileName, err)
		}

		var reqBodyPayloadBulk, reqBodyPayloadSingle []byte
		for j := range JSONObjects {
			reqBodyPayloadBulk = append(reqBodyPayloadBulk, []byte("{\"index\": {}}\n")...)
			reqBodyPayloadBulk = append(reqBodyPayloadBulk, JSONObjects[j]...)
			reqBodyPayloadBulk = append(reqBodyPayloadBulk, []byte("\n")...)
		}

		esIndex := l.fixtureFiles[i].fileNameWithoutExtension()
		reqUrl := fmt.Sprintf("%s/%s/_bulk?refresh=true", l.esUrl, esIndex)
		reqBodyBulk := bytes.NewBuffer(reqBodyPayloadBulk)
		loadRspBulk, err := cl.Post(reqUrl, "application/x-ndjson", reqBodyBulk)
		if err != nil {
			return err
		}
		_ = loadRspBulk.Body.Close()

		// temporary try to fix ES5 _bulk issue
		// todo: find a better way of doing this
		if loadRspBulk.StatusCode != http.StatusCreated && loadRspBulk.StatusCode != http.StatusOK {
			for k := range JSONObjects {
				reqBodyPayloadSingle = JSONObjects[k]
				esType := strings.Split(esIndex, "_index")
				innerIndex := esType[0]
				var loadRspSingle *http.Response
				// this is based on the fact that the index has an inner type with the same name as the index, but without "_index"
				if len(innerIndex) != 0 {
					reqUrl = fmt.Sprintf("%s/%s/%s?refresh=true", l.esUrl, esIndex, innerIndex)
					reqBodySingle := bytes.NewBuffer(reqBodyPayloadSingle)
					loadRspSingle, err = cl.Post(reqUrl, "application/json", reqBodySingle)
					if err != nil {
						return err
					}
				} else {
					// try to index without inner type
					reqUrl = fmt.Sprintf("%s/%s?refresh=true", l.esUrl, esIndex)
					reqBodySingle := bytes.NewBuffer(reqBodyPayloadSingle)
					loadRspSingle, err = cl.Post(reqUrl, "application/json", reqBodySingle)
					if err != nil {
						return err
					}
				}
				if loadRspSingle.StatusCode != http.StatusCreated && loadRspSingle.StatusCode != http.StatusOK {
					err = fmt.Errorf("can't load fixture for file: %v, err: %v", l.fixtureFiles[i].fileName, loadRspSingle.Body)
				}
				_ = loadRspSingle.Body.Close()
			}
		}
		if err != nil {
			return err
		}
	}

	return err
}

// parseFilesContent parse the content of the file(s) provided in fileNames variadic
func (l *Loader) parseFilesContent(fileNames ...string) (parsedContent []fixtureFile, err error) {
	for _, f := range fileNames {
		fixture := fixtureFile{
			path:     f,
			fileName: filepath.Base(f),
		}
		fixture.content, err = ioutil.ReadFile(fixture.path)
		if err != nil {
			err = fmt.Errorf(`esfixtures: could not read file "%s": %w`, fixture.path, err)
			return
		}
		parsedContent = append(parsedContent, fixture)
	}

	return
}

// splitJSONIntoJSONCollection receive a JSON representing an object collection([{object11}, {object2}, ...]) or a single object({object}) in []byte format
// and returns a slice with all the JSON objects, each object represented as []byte
func splitJSONIntoJSONCollection(jc []byte) (jsonCollection [][]byte, err error) {
	var extractedCollection []map[string]interface{}
	err = json.Unmarshal(jc, &extractedCollection)
	if err != nil {
		var extractedObject map[string]interface{}
		err = json.Unmarshal(jc, &extractedObject)
		if err != nil {
			return jsonCollection, fmt.Errorf("invalid json provided: %v, error: %w", string(jc), err)
		}

		obj, err := json.Marshal(extractedObject)
		if err != nil {
			return jsonCollection, err
		}

		return append(jsonCollection, obj), err
	}

	for i := range extractedCollection {
		obj, err := json.Marshal(extractedCollection[i])
		if err != nil {
			return jsonCollection, err
		}
		jsonCollection = append(jsonCollection, obj)
	}

	return jsonCollection, err
}
