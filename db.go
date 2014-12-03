package merchdb

import (
	"github.com/jbooth/flotilla"
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
	rowKey := args[0]
	tableName := string(args[1])
	dbi,err := txn.DBIOpen(&tableName,0 | flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	numCols := int(len(args[2:]) / 2)
	maxColKeyLen := 0
	// figure out length of longest column
	for i := 0 ; i < numCols ; i++ {
		colKeyLen := len(args[1+(i*2)])
		if colKeyLen > maxColKeyLen {
			maxColKeyLen = colKeyLen
		}
	}
	// format rowKey byte arr section
	rowColKeyLen := 4 + len(rowKey) + maxColKeyLen
	rowColKey := make([]byte, rowColKeyLen, rowColKeyLen)
	// put len(rowKey), rowKey in our key field
	binary.LittleEndian.PutUint32(rowColKey, uint32(len(rowKey)))
	copy(rowColKey[4:], rowKey)
	rowKeyEndIdx := 4 + len(rowKey)

	// write all columns
	for i := 0 ; i < numCols ; i++ {
		colKey := args[1 + (i*2)]
		colVal := args[2 + (i*2)]

		// add colKey to our key field
		rowColKeyInsertLength := rowKeyEndIdx + len(colKey)
		copy(rowColKey[rowKeyEndIdx:],colKey)
		// insert
		txn.Put(dbi, rowColKey[:rowColKeyInsertLength], colVal, uint(0))
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
	key := make([]byte, len(args[0]) + 4, len(args[0]) + 4)
	binary.LittleEndian.PutUint32(key,uint32(len(args[0])))
	copy(key[4:], args[0])

	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	// clear all prev columns

	// scan to key
	c,err := txn.CursorOpen(dbi)
	if err != nil {
		return nil,err
	}
	// scan until we find a key that doesn't match our row
	k,_,err := c.Get(key,uint(0))
	if err != nil {
		return nil,err
	}
	for (bytes.Equal(k[:len(key)],key)) {
		err = txn.Del(dbi,k,nil)
		if err != nil {
			return nil,err
		}
		k, _, err = c.Get(nil, uint(0))
		if err != nil {
			return nil, err
		}
	}
	// insert new columns
	return PutCols(args, txn)
}


// args:
// 0: rowKey
// 1: tableName
func GetRow(args [][]byte, txn flotilla.WriteTxn)([]byte, error) {
	retSet := make([][]byte,0,0)
	// key bytes are 4 byte keyLen + keyBytes
	key := make([]byte, len(args[0]) + 4, len(args[0]) + 4)
	binary.LittleEndian.PutUint32(key,uint32(len(args[0])))
	copy(key[4:], args[0])


	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	c,err := txn.CursorOpen(dbi)
	if err != nil {
		return nil,err
	}
	// scan until we find a key that doesn't match our row
	k,v,err := c.Get(key,uint(0))
	rowKey,colKey := keyColNames(k)

	for (bytes.Equal(key,rowKey)) {
		keyVal := [][]byte{colKey,v}
		retSet = append(retSet, keyVal...)
		k, v, err = c.Get(nil, uint(0))
		if err != nil {
			return nil, err
		}
		rowKey,colKey = keyColNames(k)
	}

	return colsBytes(retSet)
}

// args:
// 0: rowKey
// 1: tableName
// 2-N: cols to fetch
func GetCols(args [][]byte, txn flotilla.Txn)([]byte,error) {
	retSet := make([][]byte,0,0)
	// key bytes are 4 byte keyLen + keyBytes
	key := make([]byte, len(args[0]) + 4, len(args[0]) + 4)
	binary.LittleEndian.PutUint32(key,uint32(len(args[0])))
	copy(key[4:], args[0])

	colsWeWant := args[2:]

	table := string(args[1])
	dbi,err := txn.DBIOpen(&table, flotilla.MDB_CREATE)
	if err != nil {
		return nil,err
	}
	c,err := txn.CursorOpen(dbi)
	if err != nil {
		return nil,err
	}
	// scan until we find a key that doesn't match our row
	k,v,err := c.Get(key,uint(0))
	rowKey,colKey := keyColNames(k)

	for (bytes.Equal(key,rowKey)) {
		if (matchesAny(colKey,colsWeWant)) {
			keyVal := [][]byte{colKey,v} // pop size off of key
			retSet = append(retSet, keyVal...)
		}
		k, v, err = c.Get(nil, uint(0))
		if err != nil {
			return nil, err
		}
		rowKey,colKey = keyColNames(k)
	}

	return colsBytes(retSet)
}

// extracts rowKey and columnKey from an mdb key
func keyColNames(mdbKey []byte) ([]byte,[]byte) {
	rowKeySize := int(binary.LittleEndian.Uint32(mdbKey))
	rowKey := mdbKey[4:4+rowKeySize]
	colKey := mdbKey[4+rowKeySize:]
	return rowKey, colKey
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

func DelRow(args [][]byte, txn flotilla.WriteTxn)([]byte,error) {
	return nil,nil
}

func ReadCols(key []byte, cols [][]byte, txn flotilla.Txn) ([][]byte, error) {
	return nil,nil
}

func ReadRow(key []byte, txn flotilla.Txn) ([][]byte, error) {
	return nil,nil
}

// allocates a new []byte to contain the values in cols,
// passed in cols are slices of 2 bytes, { (key,val), (key,val), (key, val) }
func colsBytes(cols [][]byte) ([]byte,error) {
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
		key := keyVal[0]
		val := keyVal[1]

		// key length, val length
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(key)))
		written += 4
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(key)))
		written += 4
		// key, val
		copy(ret[written:],key)
		written += len(key)
		copy(ret[written:],val)
		written += len(val)
	}
	return ret,nil
}

// wraps byte arrays around columns passed to us
func bytesCols(in []byte) ([][]byte,error) {
	read := 0
	// read length
	numCols := binary.LittleEndian.Uint32(in[read:])
	read += 4
	ret := make([][]byte, numCols, numCols)
	for i := 0 ; i < int(numCols) ; i++ {
		ret[i] = make([]byte,2,2)
		keyLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		valLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		ret[i][0] = in[read:read+int(keyLen)]
		read += keyLen
		ret[i][1] = in[read:read+int(valLen)]
		read += valLen
 	}
	return ret,nil
}
