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
	httpListen *net.Listener
	log *log.Logger
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
	h := &http.Server {
		bindAddr,
		mux,
		1 * time.Second, // read timeout
		1 * time.Second, // write timeout
		0,
		nil,
		nil,
		nil,
		lg}

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

		err := s.h.Serve(httpListen)

		if err != nil {
			_ = s.flotilla.Close()
			_ = s.httpListen.close()
			s.lg.Fatalf("Error serving http addr %s  : %s", s.http.Addr,err)
		}
	}(s)
	return s,nil

}

func (s *Server) Close() error {
	s.httpListen.Close()
	return s.f.Close()
}

// parses a url formatted like ../tableName/rowKey?col1=val1&col2=val2 into a [][]byte that our flotilla ops will work with
func parseTableRowColVals(r *http.Request) [][]byte {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	tableName := []byte(pathSplits[pathSplits.length - 2])
	rowKey := []byte(pathSplits[pathSplits.length - 1])

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
	tableName := []byte(pathSplits[pathSplits.length - 2])
	rowKey := []byte(pathSplits[pathSplits.length - 1])

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


// url is formatted like /tableName/rowKey?col1=val1&col2=val2
func (s *Server) HandlePutCols(w http.ResponseWriter, r *http.Request) {
	flotillaArgs := parseTableRowColVals(r)
	result := <- s.flotilla.Command(PUTCOLS, flotillaArgs)
	response := &PutColsResponse{true,nil}
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
	flotillaArgs := parseTableRowCols(r)
	result := <- s.flotilla.Command(PUTCOLS, flotillaArgs)
	response := &PutColsResponse{true,nil}
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

func (s *Server) HandleGetColsFast(lw http.ResponseWriter, r *http.Request) {
	// last element of resource path is rowKey
	return
}

func (s *Server) HandlePutRow(w http.ResponseWriter, r *http.Request) {
	return
}

func (s *Server) HandleGetRow(w http.ResponseWriter, r *http.Request) {
	return
}

func (s *Server) HandleGetRowFast(w http.ResponseWriter, r *http.Request) {
	return
}

func (s *Server) HandleDelRow(w http.ResponseWriter, r *http.Request) {
	return
}
