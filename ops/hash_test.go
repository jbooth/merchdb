package merchdb

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
	"os"
	"testing"
)

func TestDbFunctions(t *testing.T) {
	dbPath := "/tmp/merchDbTest"
	err := os.RemoveAll(dbPath)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(dbPath, 0755)
	if err != nil {
		panic(err)
	}
	// set up env
	env, err := mdb.NewEnv()
	env.SetMaxDBs(mdb.DBI(1024))
	env.SetMaxReaders(1024)
	err = env.Open(dbPath, mdb.CREATE, uint(0755))
	if err != nil {
		panic(err)
	}
	// new write txn
	txn, err := env.BeginTxn(nil, uint(0))
	if err != nil {
		panic(err)
	}

	table := "table"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)

	// write a couple rows
	rowKey1 := []byte("rowOne")
	rowKey2 := []byte("rowTwo")
	cols := []colKeyVal{
		colKeyVal{[]byte("colOne"), []byte("valOne")},
		colKeyVal{[]byte("colTwo"), []byte("valTwo")},
	}

	err = putCols(txn, dbi, rowKey1, cols)
	err = putCols(txn, dbi, rowKey2, cols)
	txn.Commit()
	// new read txn
	txn, err = env.BeginTxn(nil, uint(0))
	if err != nil {
		panic(err)
	}
	dbi, err = txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		panic(err)
	}
	c, err := txn.CursorOpen(dbi)
	if err != nil {
		panic(err)
	}
	doForRow(c, rowKey1, func(c colKeyVal) error {
		fmt.Printf("col key %s val %s", string(c.k), string(c.v))
		return nil
	})
	// read both rows
}
