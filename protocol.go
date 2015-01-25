package merchdb

type WriteResponse struct {
	Ok  bool
	Err error
}

type ReadResponse struct {
	Ok   bool
	Err  error
	Key  string
	Cols map[string]string
}
