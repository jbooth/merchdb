package merchdb

import (
	"github.com/jbooth/flotilla"
)


var (
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

func PutCols(args [][]byte, txn WriteTxn) ([]byte, error) {
	return nil,nil
}

func PutRow(args [][]byte, txn WriteTxn) ([]byte, error) {
	return nil,nil
}

func GetRow(args [][]byte, txn WriteTxn)([]byte, error) {
	return nil,nil
}

func GetCols(args [][]byte, txn WriteTxn)([]byte,error) {
	return nil,nil
}

func DelRow(args [][]byte, txn WriteTxn)([]byte,error) {
	return nil,nil
}

func ReadCols(key []byte, cols [][]byte, txn Txn) ([][]byte, error) {
	return nil,nil
}

func ReadRow(key []byte, txn Txn) ([][]byte, error) {
	return nil,nil
}
