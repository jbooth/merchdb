package merchdb

type PutColsResponse struct {
	Ok 		bool
	Err 	error
}

type PutRowResponse struct {
	Ok bool
	Err error
}

type GetRowResponse struct {
	Key string
	Cols map[string]string
	Ok  bool
	Err error
}

type GetColsResponse struct {
	Key string
	Cols map[string]string
	Ok  bool
	Err error
}
