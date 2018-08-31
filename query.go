package udt

// NewQuery creates a new Query object from a query string
func NewQuery(query string) *Query {
	return &Query{
		query: query,
	}
}

// Query is an object representing a query to be run on a Client
type Query struct {
	query string
}

// Run runs the query on the provided Client returning a Results object
func (q *Query) Run(client *Client) (*Results, error) {
	r, err := client.ExecutePhantom(q.query)
	if err != nil {
		return nil, err
	}

	return NewResults(r), nil
}
