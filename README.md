UDT
===

`github.com/samuelhug/udt` is a Go library for programmatic  interaction with a Unidata database.

Example:
```
go get github.com/samuelhug/udt
cd $GOPATH/src/github.com/samuelhug/udt/example
go run main.go -host <udtserver>
```

Output:
```
Enter Username: demo
Enter Password:
=== Example Basic ===
Query basic server information
Hardware         : ####
Operating system : ####
O.S. version     : #.#
UniData version  : #.#.#
Restore command  : ################
Product Serial Number : ############
PHANTOM process ######## has completed.

=== Example Query ===
Run the query: LIST CLIENTS NAME COMPANY ADDRESS SAMPLE 3 TOXML
map["_ID":"9999" "NAME":"Paul Castiglione" "COMPANY":"Chez Paul" "ADDRESS":["45, reu de Rivoli"]]
map["_ID":"10034" "NAME":"Fredrick Anderson" "COMPANY":"Otis Concrete" "ADDRESS":["854, reu de Rivoli"]]
map["_ID":"9980" "NAME":"Beverly Ostrovich" "COMPANY":"Riley Architects" "ADDRESS":["7925 S. Blake St."]]
```