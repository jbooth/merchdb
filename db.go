package merchdb

import (
	"github.com/jbooth/flotilla"
	"github.com/jbooth/flotilla/mdb"
	"fmt"
	"encoding/binary"
	"bytes"

)

// db format

// key:  4 byte uint32 for rowKey length, n bytes of rowKey, remaining bytes are column key
// val:  column value

var (
	nobytes []byte = make([]byte,0,0)
	GETCOLS string = "GetCols"
	PUTCOLS string = "PutCols"
	GETROW string = "GetRow"
	PUTROW string = "PutRow"
	DELROW string = "DelRow"

	ops  = map[string]flotilla.Command {
		GETCOLS: GetCols,
		PUTCOLS: PutCols,
		GETROW: GetRow,
		PUTROW: PutRow,
		DELROW: DelRow,
	}
)

// args:
// 0: row key
// 1: table name
// 2-N:  col key,val pairs

// outputs: nil, error state
func PutCols(args [][]byte, txn flotilla.WriteTxn) ([]byte, error) {
	// key bytes are 4 byte keyLen + keyBytes
	rowKey := args[0]
	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	// put our columns
	keyValBytes := args[2:]
	if (len(keyValBytes) % 2 != 0) {
		return nil,fmt.Errorf("Had odd number of column keyVals on insert to table %s rowKey %s", table, string(rowKey))
	}
	keyVals := make([]keyVal, len(keyValBytes) / 2, len(keyValBytes) / 2)
	for i := 0 ; i < int(keyValBytes / 2) ; i++ {
		keyVals[i] = keyVal{keyValBytes[i*2], keyValBytes[(i*2) + 1]}
	}
	err = putCols(txn, dbi, rowKey, keyVals)
	if err != nil {
		return nil,err
	}
	return nobytes,txn.Commit()
}
// PutRow clears all previously existing columns for the row, in addition to adding the provided columns
// args:
// 0: row key
// 1: table name
// 2-N:  col key,val pairs

// outputs: nil, error state
func PutRow(args [][]byte, txn flotilla.WriteTxn) ([]byte, error) {
	// key bytes are 4 byte keyLen + keyBytes
	rowKey := args[0]
	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	// clear all prev columns
	delRow(txn,dbi,rowKey)
	// put our columns
	keyValBytes := args[2:]
	if (len(keyValBytes) % 2 != 0) {
		return nil,fmt.Errorf("Had odd number of column keyVals on insert to table %s rowKey %s", table, string(rowKey))
	}
	keyVals := make([]keyVal, len(keyValBytes) / 2, len(keyValBytes) / 2)
	for i := 0 ; i < int(keyValBytes / 2) ; i++ {
		keyVals[i] = keyVal{keyValBytes[i*2], keyValBytes[(i*2) + 1]}
	}
	err = putCols(txn, dbi, rowKey, keyVals)
	if err != nil {
		return nil,err
	}
	return nobytes,txn.Commit()
}


// args:
// 0: rowKey
// 1: tableName
func GetRow(args [][]byte, txn flotilla.WriteTxn)([]byte, error) {
	retKeyVals :=
}

// args:
// 0: rowKey
// 1: tableName
// 2-N: cols to fetch
func GetCols(args [][]byte, txn flotilla.WriteTxn)([]byte,error) {
	retSet := make([][]byte,0,0)
	// key bytes are 4 byte keyLen + keyBytes

	key := make([]byte, len(args[0]) + 4, len(args[0]) + 4)
	binary.LittleEndian.PutUint32(key,uint32(len(args[0])))
	copy(key[4:], args[0])

	rowKey := args[0]
	table := string(args[1])
	var colsWeWant [][]byte = nil
	if len(args) > 2 {
		colsWeWant = args[2:]
	}

	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}

	cols,err := getCols(txn, dbi, rowKey, colsWeWant)
	if err != nil {
		return nil,err
	}
	txn.Abort() // abort since we're not writing
	return colsBytes(cols)
}


// args:
// 0: rowKey
// 1: tableName
func DelRow(args [][]byte, txn flotilla.WriteTxn)([]byte,error) {
	rowKey := args[0]
	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	return nobytes,delRow(txn, dbi, rowKey)
}

func ReadCols(key []byte, cols [][]byte, txn flotilla.Txn) ([][]byte, error) {
	return nil,nil
}

func ReadRow(key []byte, txn flotilla.Txn) ([][]byte, error) {
	return nil,nil
}

func delRow(txn flotilla.WriteTxn, dbi mdb.DBI, rowKey []byte) (error) {

	// key bytes are 4 byte keyLen + keyBytes
	seekKey := make([]byte, len(rowKey) + 4, len(rowKey) + 4)
	binary.LittleEndian.PutUint32(seekKey,uint32(len(rowKey)))
	copy(seekKey[4:], rowKey)

	c,err := txn.CursorOpen(dbi)
	if err != nil {
		return err
	}
	// scan until we find a key that doesn't match our row
	k,v,err := c.Get(seekKey,uint(0))
	sRowK,sColK := keyColNames(k)

	// if we're still in this row
	for (bytes.Equal(sRowK,rowKey)) {
		// this col belongs to our row, kill it
		err = txn.Del(dbi, k, nil)
		if err != nil {
			return err
		}

		// load next k,v
		k, v, err = c.Get(nil, uint(0))
		if err != nil {
			return err
		}
		sRowK,sColK = keyColNames(k)
	}

	return nil
}


func putCols(txn flotilla.WriteTxn, dbi mdb.DBI, rowKey []byte, cols []keyVal) error {
	for _,col := range cols {
		putKey := packKeyCol(rowKey,col.k)
		txn.Put(dbi,putKey,col.v,uint(0))
	}
}

// if cols is nil, returns whole row -- otherwise returns only those with colKeys selected in cols
// returns pairs of (colKey, colVal) with err
func getCols(txn flotilla.Txn, dbi mdb.DBI, rowKey []byte, cols [][]byte) ([]keyVal, error) {

	// key bytes are 4 byte keyLen + keyBytes
	seekKey := make([]byte, len(rowKey) + 4, len(rowKey) + 4)
	binary.LittleEndian.PutUint32(seekKey,uint32(len(rowKey)))
	copy(seekKey[4:], rowKey)

	retSet := make([]keyVal,0,0)
	c,err := txn.CursorOpen(dbi)
	if err != nil {
		return nil,err
	}
	// scan until we find a key that doesn't match our row
	k,v,err := c.Get(seekKey,uint(0))
	sRowK,sColK := keyColNames(k)

	// if we're still in this row
	for (bytes.Equal(sRowK,rowKey)) {
		// if this is a col we want, add it
		if (cols != nil && matchesAny(sColK,cols)) {
			keyVal := keyVal{sColK,v} // pop size off of key
			retSet = append(retSet, keyVal...)
		}
		// load next k,v
		k, v, err = c.Get(nil, uint(0))
		if err != nil {
			return nil, err
		}
		sRowK,sColK = keyColNames(k)
	}

	return retSet,nil
}

// extracts rowKey and columnKey from an mdb key
func keyColNames(mdbKey []byte) ([]byte,[]byte) {
	rowKeySize := int(binary.LittleEndian.Uint32(mdbKey))
	rowKey := mdbKey[4:4+rowKeySize]
	colKey := mdbKey[4+rowKeySize:]
	return rowKey, colKey
}


// packs key & col into a single key for mdb
func packKeyCol(rowKey []byte, colKey []byte) ([]byte) {
	mdbKey := make([]byte, 4 + len(rowKey) + len(colKey), 4 + len(rowKey) + len(colKey))

	binary.LittleEndian.PutUint32(mdbKey,uint32(len(rowKey)))
	copy(mdbKey[4:], rowKey)
	copy(mdbKey[4+len(rowKey):], colKey)
	return mdbKey
}

func matchesAny(needle []byte, hayStack [][]byte) bool {
	for _,h := range hayStack {
		if (len(h) == len(needle)) {
			matched := true
			for idx,b := range h {
				if (b != needle[idx]) {
					matched = false
					break
				}
			}
			if (matched) { return true }
		}
	}
	return false
}

type keyVal struct {
	k []byte
	v []byte
}

// allocates a new []byte to contain the values in cols,
// passed in cols are slices of 2 bytes, { (key,val), (key,val), (key, val) }
func colsBytes(cols []keyVal) ([]byte,error) {
	retLength := 4 + (8 * len(cols))
	for _,keyVal := range(cols) {
		if (len(keyVal) != 2) {
			return nil,fmt.Errorf()
		}
		retLength += len(keyVal[0])
		retLength += len(keyVal[1])
	}
	ret := make([]byte,retLength,retLength)
	written := 0
	// write num records
	binary.LittleEndian.PutUint32(ret[written:],uint32(len(cols)))
	written += 4
	// for each record
	for _,keyVal := range(cols) {

		// key length, val length
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(keyVal.k)))
		written += 4
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(keyVal.v)))
		written += 4
		// key, val
		copy(ret[written:],keyVal.k)
		written += len(keyVal.k)
		copy(ret[written:],keyVal.v)
		written += len(keyVal.v)
	}
	return ret,nil
}

// wraps byte arrays around columns passed to us
func bytesCols(in []byte) ([]keyVal,error) {
	read := 0
	// read length
	numCols := binary.LittleEndian.Uint32(in[read:])
	read += 4
	ret := make([]keyVal, numCols, numCols)
	for i := 0 ; i < int(numCols) ; i++ {
		ret[i] = make([]byte,2,2)
		keyLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		valLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		k := in[read:read+int(keyLen)]
		v := in[read:read+int(valLen)]
		ret[i] = keyVal{k,v}
 	}
	return ret,nil
}
