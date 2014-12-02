package merchdb

import (
	"github.com/jbooth/flotilla"
	"net/http"
	"time"
	"log"
	"os"
	"strings"
)

type Server struct {
	flotilla flotilla.DB
	http *http.Server
}

func NewServer(bindAddr string, dataDir string, flotillaPeers []string, httpPort int) (*Server, error) {
	log := log.New(os.Stderr, "MerchDB:\t",log.LstdFlags)
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
		log}


	go func() {
		err := h.ListenAndServe()
		if err != nil {
			_ = f.Close()
			log.Fatalf("Error serving http addr %s  : %s", bindAddr,err)
		}
	}()

	return &Server{f,h},nil

}

func (s *Server) Close() error {
	return s.f.Close()
}

func (s *Server) HandlePutCols(w http.ResponseWriter, r *http.Request) {
	// last element of resource path is rowKey
	pathSplits := strings.Split(r.URL.Path, "/")
	rowKey := []byte(pathSplits[pathSplits.length - 1])

	// url params are columns
	numCols := len(r.Form)
	// args for flotilla are rowKey [colKey, colVal]...
	flotillaArgs := make([][]byte, (numCols*2) + 1)
	flotillaArgs[] = rowKey
	i := 1
	for k,v := range r.Form {
		flotillaArgs[i] = []byte(k)
		i++
		flotillaArgs[i] = []byte(v)
		i++
	}
	result := <- s.flotilla.Command(PUTCOLS, flotillaArgs)
	w.Write(result.bytes())
}

func (s *Server) HandleGetCols(w http.ResponseWriter, r *http.Request) {
	// last element of resource path is rowKey
}

func (s *Server) HandlePutRow(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) HandleGetRow(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) HandleDelRow(w http.ResponseWriter, r *http.Request) {

}
