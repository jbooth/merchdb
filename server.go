package merchdb

import (
	"github.com/jbooth/flotilla"
	"net/http"
	"time"
	"log"
	"os"
	"strings"
	"encoding/json"
	"net"
)

type Server struct {
	flotilla flotilla.DB
	http *http.Server
	httpListen net.Listener
	lg *log.Logger
}

func NewServer(bindAddr string, dataDir string, flotillaPeers []string) (*Server, error) {
	lg := log.New(os.Stderr, "MerchDB:\t",log.LstdFlags)
	// start flotilla
	// peers []string, dataDir string, bindAddr string, ops map[string]Command
	f,err := flotilla.NewDefaultDB(flotillaPeers, dataDir, bindAddr, ops)
	if err != nil {
		return nil,err
	}
	// register http methods
	mux := http.NewServeMux()

	// start http server
	h := &http.Server{}
	h.ErrorLog = lg
	h.Addr = bindAddr
	h.Handler = mux
	h.ReadTimeout = 1 * time.Second
	h.WriteTimeout = 1 * time.Second

	httpAddr,err := net.ResolveTCPAddr(bindAddr, "tcp4")
	if err != nil {
		return nil,err
	}
	httpListen,err := net.ListenTCP("tcp4", httpAddr)
	if err != nil {
		return nil,err
	}
	s :=  &Server{f,h,httpListen,lg}
	go func(s *Server) {

		err := s.http.Serve(httpListen)

		if err != nil {
			_ = s.flotilla.Close()
			_ = s.httpListen.Close()
			s.lg.Fatalf("Error serving http addr %s  : %s", s.http.Addr,err)
		}
	}(s)
	return s,nil

}

func (s *Server) Close() error {
	s.httpListen.Close()
	return s.flotilla.Close()
}

// parses a url formatted like ../tableName/rowKey?col1=val1&col2=val2 into a [][]byte that our flotilla ops will work with
func parseTableRowColVals(r *http.Request) [][]byte {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits) - 2])
	rowKey := []byte(pathSplits[len(pathSplits) - 1])

	// url params are columns
	numCols := len(r.Form)
	// args for flotilla are rowKey [colKey, colVal]...
	flotillaArgs := make([][]byte, (numCols*2) + 2)
	flotillaArgs[0] = rowKey
	flotillaArgs[1] = tableName
	i := 1
	for k,v := range r.Form {
		flotillaArgs[i] = []byte(k)
		i++
		flotillaArgs[i] = []byte(v)
		i++
	}
	return flotillaArgs
}

// parses a url formatted like ../tableName/rowKey?col1=whatev&col2=whatever into a [][]byte that our flotilla ops will work with,
// ignores valuse (intended for getCols requests)
func parseTableRowColNames(r *http.Request) [][]byte {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits) - 2])
	rowKey := []byte(pathSplits[len(pathSplits) - 1])

	// url params are columns
	numCols := len(r.Form)
	// args for flotilla are rowKey [colKey, colVal]...
	flotillaArgs := make([][]byte, numCols + 2)
	flotillaArgs[0] = rowKey
	flotillaArgs[1] = tableName
	i := 1
	for k,v := range r.Form {
		flotillaArgs[i] = []byte(k)
		i++
	}
	return flotillaArgs
}

// only parses table name and row key, ignoring any CGI params
func parseTableRowKey(r *http.Request) [][]byte {
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[len(pathSplits) - 2])
	rowKey := []byte(pathSplits[len(pathSplits) - 1])
	return [][]byte{tableName,rowKey}
}


// url is formatted like /tableName/rowKey?col1=val1&col2=val2
func (s *Server) HandlePutCols(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColVals(r)
	result := <- s.flotilla.Command(PUTCOLS, flotillaArgs)
	response := &WriteResponse{true,nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}

}

func (s *Server) HandleGetCols(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColNames(r)
	result := <- s.flotilla.Command(GETCOLS, flotillaArgs)
	response := &ReadResponse{}

	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	} else {
		resultCols,err := bytesCols(result.Response)
		if err != nil {
			response.Ok = false
			response.Err = err
		} else {
			response.Ok = true
			response.Key = string(flotillaArgs[0])
			response.Cols = make(map[string]string)
			for _,keyVal := range resultCols {
				response.Cols[string(keyVal.k)] = string(keyVal.v)
			}
		}
	}
	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}
}

func (s *Server) HandleGetColsFast(w http.ResponseWriter, r *http.Request) {

	flotillaArgs := parseTableRowColNames(r)
	txn,err := s.flotilla.Read()
	if err != nil {
		return returnErr(w,err)
	}
	// rowKey is args[0], tableName is args[1]
	rowKey := flotillaArgs[0]
	tableName := string(flotillaArgs[1])
	colNames := flotillaArgs[2:]
	dbi,err := txn.DBIOpen(&tableName, flotilla.MDB_CREATE)
	if err != nil {
		return returnErr(w,err)
	}
	results,err := getCols(txn, dbi, rowKey, colNames)

	response := &ReadResponse{}
	if err != nil {
		response.Ok = false
		response.Err = err
		return
	} else {
		response.Key = string(rowKey)
		response.Ok = true
		response.Cols = make(map[string]string)
		for _,keyVal := range results {
			response.Cols[string(keyVal.k)] = string(keyVal.v)
		}
	}

	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}
	return
}

func (s *Server) HandlePutRow(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColVals(r)
	result := <- s.flotilla.Command(PUTROW, flotillaArgs)
	response := &WriteResponse{true,nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}
}

func (s *Server) HandleGetRow(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowKey(r)
	result := <- s.flotilla.Command(GETROW, flotillaArgs)
	response := &ReadResponse{}

	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	} else {
		resultCols,err := bytesCols(result.Response)
		if err != nil {
			response.Ok = false
			response.Err = err
		} else {
			response.Ok = true
			response.Key = string(flotillaArgs[0])
			response.Cols = make(map[string]string)
			for _,keyVal := range resultCols {
				response.Cols[string(keyVal.k)] = string(keyVal.v)
			}
		}
	}
	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}
	return
}

func (s *Server) HandleGetRowFast(w http.ResponseWriter, r *http.Request) {

	flotillaArgs := parseTableRowKey(r)
	txn,err := s.flotilla.Read()
	if err != nil {
		return returnErr(w,err)
	}
	// rowKey is args[0], tableName is args[1]
	rowKey := flotillaArgs[0]
	tableName := string(flotillaArgs[1])
	dbi,err := txn.DBIOpen(&tableName, flotilla.MDB_CREATE)
	if err != nil {
		return returnErr(w,err)
	}
	results,err := getCols(txn, dbi, rowKey, nil)

	response := &ReadResponse{}
	if err != nil {
		response.Ok = false
		response.Err = err
		return
	} else {
		response.Key = string(rowKey)
		response.Ok = true
		response.Cols = make(map[string]string)
		for _,keyVal := range results {
			response.Cols[string(keyVal.k)] = string(keyVal.v)
		}
	}

	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}
	return
}

func (s *Server) HandleDelRow(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowKey(r)
	result := <- s.flotilla.Command(DELROW, flotillaArgs)
	response := &WriteResponse{true,nil}
	if result.Err != nil {
		response.Ok = false
		response.Err = result.Err
	}
	w.Header().Add("Content-Type","application-json")
	enc := json.NewEncoder(w)
	err := enc.Encode(response)
	if err != nil {
		s.lg.Errorf(err)
	}

}

func returnErr(w http.ResponseWriter, err error) {
	w.WriteHeader(500)
	w.Write([]byte(err))
}
