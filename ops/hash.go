package ops

import (
	"bytes"
	"encoding/binary"
	"fmt"
	mdb "github.com/jbooth/gomdb"
)

// db format

// key:  4 byte uint32 for rowKey length, n bytes of rowKey, remaining bytes are column key
// val:  column value

// args:
// 0: row key
// 1: table name
// 2-N:  col key,val pairs

// outputs: nil, error state
func PutCols(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	fmt.Printf("PutCols(args = [%#v], txn = [%#v])\n", args, txn)
	fmt.Printf("Executing putcols! \n")
	// key bytes are 4 byte keyLen + keyBytes
	rowKey := args[0]
	table := string(args[1])
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	// put our columns
	keyValBytes := args[2:]
	if len(keyValBytes)%2 != 0 {
		return nil, fmt.Errorf("Had odd number of column keyVals on insert to table %s rowKey %s", table, string(rowKey))
	}
	keyVals := make([]colKeyVal, len(keyValBytes)/2, len(keyValBytes)/2)
	for i := 0; i < int(len(keyValBytes)/2); i++ {
		keyVals[i] = colKeyVal{keyValBytes[i*2], keyValBytes[(i*2)+1]}
	}
	err = putCols(txn, dbi, rowKey, keyVals)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	return nobytes, txn.Commit()
}

// PutRow clears all previously existing columns for the row, in addition to adding the provided columns
// args:
// 0: row key
// 1: table name
// 2-N:  col key,val pairs

// outputs: nil, error state
func PutRow(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	fmt.Printf("PutRow(args = [%#v], txn = [%#v])\n", args, txn)
	// key bytes are 4 byte keyLen + keyBytes
	rowKey := args[0]
	table := string(args[1])
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	// clear all prev columns
	delRow(txn, dbi, rowKey)
	// put our columns
	keyValBytes := args[2:]
	if len(keyValBytes)%2 != 0 {
		return nil, fmt.Errorf("Had odd number of column keyVals on insert to table %s rowKey %s", table, string(rowKey))
	}
	keyVals := make([]colKeyVal, len(keyValBytes)/2, len(keyValBytes)/2)
	for i := 0; i < int(len(keyValBytes)/2); i++ {
		keyVals[i] = colKeyVal{keyValBytes[i*2], keyValBytes[(i*2)+1]}
	}
	err = putCols(txn, dbi, rowKey, keyVals)
	if err != nil {
		return nil, err
	}
	return nobytes, txn.Commit()
}

// args:
// 0: rowKey
// 1: tableName
func GetRow(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	fmt.Printf("GetRow(args = [%#v], txn = [%#v])\n", args, txn)
	rowKey := args[0]
	table := string(args[1])
	fmt.Println("Executing getRow, opening dbi")
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	retKeyVals, err := getCols(txn, dbi, rowKey, nil)
	fmt.Printf("Executing getrow for key %s got err %s", string(rowKey), err)
	if err != nil {
		return nil, err
	}
	return colsBytes(retKeyVals)
}

// args:
// 0: rowKey
// 1: tableName
// 2-N: cols to fetch
func GetCols(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	fmt.Printf("GetCols(args = [%#v], txn = [%#v])\n", args, txn)
	rowKey := args[0]
	table := string(args[1])
	var colsWeWant [][]byte = nil
	if len(args) > 2 {
		colsWeWant = args[2:]
	}

	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}

	cols, err := getCols(txn, dbi, rowKey, colsWeWant)
	if err != nil {
		return nil, err
	}
	txn.Abort() // abort since we're not writing
	return colsBytes(cols)
}

// args:
// 0: rowKey
// 1: tableName
func DelRow(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	rowKey := args[0]
	table := string(args[1])
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	return nobytes, delRow(txn, dbi, rowKey)
}

func delRow(txn *mdb.Txn, dbi mdb.DBI, rowKey []byte) error {

	// key bytes are 4 byte keyLen + keyBytes
	seekKey := make([]byte, len(rowKey)+4, len(rowKey)+4)
	binary.LittleEndian.PutUint32(seekKey, uint32(len(rowKey)))
	copy(seekKey[4:], rowKey)

	c, err := txn.CursorOpen(dbi)
	if err != nil {
		return err
	}
	return doForRow(c, rowKey, func(ckv colKeyVal) error {
		mdbKey := packRowColKey(rowColKey{rowKey, ckv.k})
		err = txn.Del(dbi, mdbKey, nil)
		return err
	})
}

// applies forCol to each colKey/colVal pair in a row, bailing early if any error reached
func doForRow(c *mdb.Cursor, rowKey []byte, forCol func(colKeyVal) error) error {
	// seek to first item (empty col name) for this rowKey
	rcSeekKey := rowColKey{rowKey, make([]byte, 0)}
	seekKey := packRowColKey(rcSeekKey)

	//fmt.Printf("Seeking to key %X\n", seekKey)
	_, _, err := c.Get(seekKey, mdb.SET_RANGE)
	if err != nil {
		return fmt.Errorf("Error while seeking in doForRow: %s", err)
	}

	for {
		// get current record
		k, v, err := c.Get(nil, mdb.GET_CURRENT)
		if err != nil {
			return fmt.Errorf("Error while fetching in doForRow: %s", err)
		}
		// check if we're still on same rowkey
		rcKey := splitRowColKey(k)
		if !bytes.Equal(rowKey, rcKey.rowKey) {
			// finished this row, bail out
			return nil
		}
		ckVal := colKeyVal{rcKey.colKey, v}
		err = forCol(ckVal)
		if err != nil {
			return fmt.Errorf("Error applying in doForRow: %s", err)
		}

		// advance cursor
		_, _, err = c.Get(nil, mdb.NEXT)
		if err != nil {
			return fmt.Errorf("Error advancing cursor in doForRow: %s", err)
		}
	}
	// unreachable
	return nil
}

func putCols(txn *mdb.Txn, dbi mdb.DBI, rowKey []byte, cols []colKeyVal) error {
	var err error = nil
	for _, col := range cols {
		putKey := packRowColKey(rowColKey{rowKey, col.k})
		fmt.Printf("Actual put of key %#v val %#v", putKey, col.v)
		err = txn.Put(dbi, putKey, col.v, uint(0))
		if err != nil {
			return err
		}
	}
	return nil
}

// if cols is nil, returns whole row -- otherwise returns only those with colKeys selected in cols
// returns pairs of (colKey, colVal) with err
func getCols(txn *mdb.Txn, dbi mdb.DBI, rowKey []byte, cols [][]byte) ([]colKeyVal, error) {
	fmt.Printf("getCols(txn = [%#v], dbi = [%#v], rowKey = [%s], cols = [%#v])\n", txn, dbi, string(rowKey), cols)
	// key bytes are 4 byte keyLen + keyBytes
	seekKey := make([]byte, len(rowKey)+4, len(rowKey)+4)
	binary.LittleEndian.PutUint32(seekKey, uint32(len(rowKey)))
	copy(seekKey[4:], rowKey)

	retSet := make([]colKeyVal, 0, 0)
	c, err := txn.CursorOpen(dbi)
	defer c.Close()
	if err != nil {
		return nil, err
	}

	// todo use doForCols
	return retSet, nil
}

type rowColKey struct {
	rowKey []byte
	colKey []byte
}

// packs a rowKey and colKey into a single []byte for an mdb key
func packRowColKey(in rowColKey) []byte {
	keyLen := 4 + len(in.rowKey) + 4 + len(in.colKey)
	mdbKey := make([]byte, keyLen)
	binary.LittleEndian.PutUint32(mdbKey, uint32(len(in.rowKey)))
	copy(mdbKey[4:], in.rowKey)
	copy(mdbKey[4+len(in.rowKey):], in.colKey)
	return mdbKey
}

func splitRowColKey(mdbKey []byte) rowColKey {
	rowKeySize := int(binary.LittleEndian.Uint32(mdbKey))
	rowKey := mdbKey[4 : 4+rowKeySize]
	colKey := mdbKey[4+rowKeySize:]
	return rowColKey{rowKey, colKey}
}

func matchesAny(needle []byte, hayStack [][]byte) bool {
	for _, h := range hayStack {
		if len(h) == len(needle) {
			matched := true
			for idx, b := range h {
				if b != needle[idx] {
					matched = false
					break
				}
			}
			if matched {
				return true
			}
		}
	}
	return false
}

// represents a column within a row
type colKeyVal struct {
	k []byte
	v []byte
}

func (k *colKeyVal) String() string {
	return string(k.k) + "\t" + string(k.v)
}

// allocates a new []byte to contain the values in cols,
// passed in cols are slices of 2 bytes, { (key,val), (key,val), (key, val) }
func colsBytes(cols []colKeyVal) ([]byte, error) {
	retLength := 4 + (8 * len(cols))
	for _, keyVal := range cols {
		retLength += len(keyVal.k)
		retLength += len(keyVal.v)
	}
	ret := make([]byte, retLength, retLength)
	written := 0
	// write num records
	binary.LittleEndian.PutUint32(ret[written:], uint32(len(cols)))
	written += 4
	// for each record
	for _, keyVal := range cols {

		// key length, val length
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(keyVal.k)))
		written += 4
		binary.LittleEndian.PutUint32(ret[written:], uint32(len(keyVal.v)))
		written += 4
		// key, val
		copy(ret[written:], keyVal.k)
		written += len(keyVal.k)
		copy(ret[written:], keyVal.v)
		written += len(keyVal.v)
	}
	return ret, nil
}

// wraps byte arrays around columns passed to us
func bytesCols(in []byte) ([]colKeyVal, error) {
	if in == nil || len(in) == 0 {
		return []colKeyVal{}, nil
	}
	read := 0
	// read length
	numCols := binary.LittleEndian.Uint32(in[read:])
	read += 4
	ret := make([]colKeyVal, numCols, numCols)
	for i := 0; i < int(numCols); i++ {
		keyLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		valLen := binary.LittleEndian.Uint32(in[read:])
		read += 4
		k := in[read : read+int(keyLen)]
		v := in[read : read+int(valLen)]
		ret[i] = colKeyVal{k, v}
	}
	return ret, nil
}
