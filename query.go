package udt

func NewQuery(query string) *Query {
	return &Query{
		query: query,
	}
}

type Query struct {
	query string
}

func (q *Query) Run(conn *Connection) (*Results, error) {
	proc, err := conn.ExecutePhantom(q.query)
	if err != nil {
		return nil, err
	}

	if err := conn.Wait(proc); err != nil {
		return nil, err
	}

	r, err := conn.RetreiveOutput(proc)
	if err != nil {
		return nil, err
	}

	return NewResults(r), nil
}
