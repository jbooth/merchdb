package merchdb

import (
	"fmt"
	"os"
	"testing"
)

func TestMerchDB(t *testing.T) {
	os.RemoveAll("/tmp/merchdbTest")
	os.MkdirAll("/tmp/merchdbTest", os.FileMode(0777))

	homeDirs := []string{"/tmp/merchdbTest/1", "/tmp/merchdbTest/2", "/tmp/merchdbTest/3"}
	webAddrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	flotAddrs := []string{"localhost:1101", "localhost:1102", "localhost:1103"}

	dbs := make([]*Server, 3)
	waitingUp := make([]chan error, 3)
	for i := 0; i < 3; i++ {
		waitingUp[i] = make(chan error)
	}

	// start'em
	for i := 0; i < 3; i++ {
		go func(j int) {
			fmt.Printf("Starting db %d\n", j)
			var err error
			err = os.MkdirAll(homeDirs[j], os.FileMode(0777))
			if err != nil {
				panic(err)
			}
			dbs[j], err = NewServer(webAddrs[j], flotAddrs[j], homeDirs[j], flotAddrs)
			fmt.Printf("Pushing err to chan for %d\n", j)
			waitingUp[j] <- err
			fmt.Printf("Sent err to chan for %d\n", j)
		}(i)
	}

	// wait all up
	for i := 0; i < 3; i++ {
		fmt.Printf("Waiting on server %d\n", i)
		err := <-waitingUp[i]
		if err != nil {
			fmt.Printf("Error starting server %d : %s", i, err)
			panic(err)
		}
	}

	fmt.Printf("All servers up\n")

	//	// test some column PUTs and a get
	//	http.Get("http://localhost:8001/putCols/table1/row1?col1=val1&col2=val2")
	//	http.Get("http://localhost:8002/putCols/table1/row1?col3=val3&col4=val4")
	//
	//	row,err := http.Get("http://localHost:8003/getRow/table1/row1")
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	fmt.Print(row)
	//
	//	// test get of bad key
	//	row,err = http.Get("http://localHost:8003/getRow/table1/badrow")
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	fmt.Print(row)

	// shut'em down
	for i := 0; i < 3; i++ {
		fmt.Printf("Killing server %d\n", i)
		dbs[i].Close()
	}

}
