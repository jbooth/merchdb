// ops for the redis-alike db stored here
// files: ops
// string.go
// 	SET
// 	GET
// hash.go
//	HSET
//	HGET
package ops

import (
	"github.com/jbooth/flotilla"
)

var (
	nobytes []byte = make([]byte, 0, 0)
	GETCOLS string = "GetCols"
	PUTCOLS string = "PutCols"
	GETROW  string = "GetRow"
	PUTROW  string = "PutRow"
	DELROW  string = "DelRow"

	Ops map[string]flotilla.Command = map[string]flotilla.Command{
		GETCOLS: GetCols,
		PUTCOLS: PutCols,
		GETROW:  GetRow,
		PUTROW:  PutRow,
		DELROW:  DelRow,
	}
)
