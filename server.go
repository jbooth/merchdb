package merchdb

import (
	"encoding/json"
	"fmt"
	"github.com/jbooth/flotilla"
	mdb "github.com/jbooth/gomdb"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type Server struct {
	flotilla   flotilla.DefaultOpsDB
	http       *http.Server
	httpListen net.Listener
	lg         *log.Logger
}

func NewServer(webAddr string, flotillaAddr string, dataDir string, flotillaPeers []string) (*Server, error) {
	lg := log.New(os.Stderr, "MerchDB:\t", log.LstdFlags)
	// start flotilla
	// peers []string, dataDir string, bindAddr string, ops map[string]Command
	f, err := flotilla.NewDefaultDB(flotillaPeers, dataDir, flotillaAddr, ops)
	if err != nil {
		return nil, err
	}
	// register http methods
	mux := http.NewServeMux()

	// start http server
	h := &http.Server{}
	h.ErrorLog = lg
	h.Addr = webAddr
	h.Handler = mux
	h.ReadTimeout = 1 * time.Second
	h.WriteTimeout = 1 * time.Second

	httpAddr, err := net.ResolveTCPAddr("tcp4", webAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't resolve webAddr %s : %s", webAddr, err)
	}
	httpListen, err := net.ListenTCP("tcp4", httpAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't bind  to httpAddr %s", httpAddr, err)
	}
	s := &Server{f, h, httpListen, lg}

	mux.HandleFunc("/putCols/", s.HandlePutCols)
	mux.HandleFunc("/putRow/", s.HandlePutRow)
	mux.HandleFunc("/getRow/", s.HandleGetRow)
	mux.HandleFunc("/delRow/", s.HandleDelRow)

	go func(s *Server) {

		err := s.http.Serve(httpListen)

		if err != nil {
			_ = s.flotilla.Close()
			_ = s.httpListen.Close()
			s.lg.Printf("Error serving http addr %s  : %s", s.http.Addr, err)
		}
	}(s)
	return s, nil

}

func (s *Server) Close() error {
	s.httpListen.Close()
	return s.flotilla.Close()
}

// parses a url formatted like ../tableName/rowKey?col1=val1&col2=val2 into a [][]byte that our flotilla ops will work with
func parseTableRowColVals(r *http.Request) [][]byte {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits)-2])
	rowKey := []byte(pathSplits[len(pathSplits)-1])

	// url params are columns
	numCols := len(r.Form)
	// args for flotilla are rowKey [colKey, colVal]...
	flotillaArgs := make([][]byte, (numCols*2)+2)
	flotillaArgs[0] = rowKey
	flotillaArgs[1] = tableName
	i := 1
	for k, v := range r.Form {
		flotillaArgs[i] = []byte(k)
		i++
		flotillaArgs[i] = []byte(v[0])
		i++
	}
	return flotillaArgs
}

// parses a url formatted like ../tableName/rowKey?col1=whatev&col2=whatever into a [][]byte that our flotilla ops will work with,
// ignores valuse (intended for getCols requests)
func parseTableRowColNames(r *http.Request) [][]byte {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits)-2])
	rowKey := []byte(pathSplits[len(pathSplits)-1])

	// url params are columns
	numCols := len(r.Form)
	// args for flotilla are rowKey [colKey, colVal]...
	flotillaArgs := make([][]byte, numCols+2)
	flotillaArgs[0] = rowKey
	flotillaArgs[1] = tableName
	i := 1
	for k, _ := range r.Form {
		flotillaArgs[i] = []byte(k)
		i++
	}
	return flotillaArgs
}

// only parses table name and row key, ignoring any CGI params
func parseTableRowKey(r *http.Request) [][]byte {
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits)-2])
	rowKey := []byte(pathSplits[len(pathSplits)-1])
	return [][]byte{rowKey, tableName}
}

// url is formatted like /tableName/rowKey?col1=val1&col2=val2
func (s *Server) HandlePutCols(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColVals(r)
	result := <-s.flotilla.Command(PUTCOLS, flotillaArgs)
	response := &WriteResponse{true, nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}

}

func (s *Server) HandleGetCols(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColNames(r)
	result := <-s.flotilla.Command(GETCOLS, flotillaArgs)
	response := &ReadResponse{}

	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	} else {
		resultCols, err := bytesCols(result.Response)
		if err != nil {
			response.Ok = false
			response.Err = err
		} else {
			response.Ok = true
			response.Key = string(flotillaArgs[0])
			response.Cols = make(map[string]string)
			for _, keyVal := range resultCols {
				response.Cols[string(keyVal.k)] = string(keyVal.v)
			}
		}
	}
	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}
}

func (s *Server) HandleGetColsFast(w http.ResponseWriter, r *http.Request) {

	flotillaArgs := parseTableRowColNames(r)
	txn, err := s.flotilla.Read()
	if err != nil {
		returnErr(w, err)
		return
	}
	// rowKey is args[0], tableName is args[1]
	rowKey := flotillaArgs[0]
	tableName := string(flotillaArgs[1])
	colNames := flotillaArgs[2:]
	dbi, err := txn.DBIOpen(&tableName, mdb.CREATE)
	if err != nil {
		returnErr(w, err)
		return
	}
	results, err := getCols(txn, dbi, rowKey, colNames)

	response := &ReadResponse{}
	if err != nil {
		response.Ok = false
		response.Err = err
		return
	} else {
		response.Key = string(rowKey)
		response.Ok = true
		response.Cols = make(map[string]string)
		for _, keyVal := range results {
			response.Cols[string(keyVal.k)] = string(keyVal.v)
		}
	}

	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err = enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}
	return
}

func (s *Server) HandlePutRow(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColVals(r)
	result := <-s.flotilla.Command(PUTROW, flotillaArgs)
	response := &WriteResponse{true, nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}
}

func (s *Server) HandleGetRow(w http.ResponseWriter, r *http.Request) {

	flotillaArgs := parseTableRowKey(r)
	result := <-s.flotilla.Command(GETROW, flotillaArgs)
	response := &ReadResponse{}

	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	} else {
		resultCols, err := bytesCols(result.Response)
		if err != nil {
			s.lg.Printf("Error in getRow: %s", err)
			response.Ok = false
			response.Err = err
		} else {
			response.Ok = true
			response.Key = string(flotillaArgs[0])
			response.Cols = make(map[string]string)
			for _, keyVal := range resultCols {
				response.Cols[string(keyVal.k)] = string(keyVal.v)
			}
		}
	}
	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}

	return
}

func (s *Server) HandleGetRowFast(w http.ResponseWriter, r *http.Request) {

	flotillaArgs := parseTableRowKey(r)
	txn, err := s.flotilla.Read()
	if err != nil {
		returnErr(w, err)
		return
	}
	// rowKey is args[0], tableName is args[1]
	rowKey := flotillaArgs[0]
	tableName := string(flotillaArgs[1])
	dbi, err := txn.DBIOpen(&tableName, mdb.CREATE)
	if err != nil {
		returnErr(w, err)
		return
	}
	results, err := getCols(txn, dbi, rowKey, nil)

	response := &ReadResponse{}
	if err != nil {
		response.Ok = false
		response.Err = err
		return
	} else {
		response.Key = string(rowKey)
		response.Ok = true
		response.Cols = make(map[string]string)
		for _, keyVal := range results {
			response.Cols[string(keyVal.k)] = string(keyVal.v)
		}
	}

	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err = enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}
	return
}

func (s *Server) HandleDelRow(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowKey(r)
	result := <-s.flotilla.Command(DELROW, flotillaArgs)
	response := &WriteResponse{true, nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type", "application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Printf(err.Error())
	}
}

func returnErr(w http.ResponseWriter, err error) {
	w.WriteHeader(500)
	w.Write([]byte(err.Error()))
}
