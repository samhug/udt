UDT
===

`github.com/samhug/udt` is a Go library for programmatic interaction with a Unidata database.

Example:
```
go get github.com/samhug/udt
cd $GOPATH/src/github.com/samhug/udt/demo
go run main.go -host <udtserver>
```

Output:
```
Enter Username: demo
Enter Password:

=== Demo Basic ===
=== Execute a raw statement and retrieve the output
Hardware         : ####
Operating system : ####
O.S. version     : #.#
UniData version  : #.#.#
Restore command  : ################
Product Serial Number : ############

=== Demo Query ===
=== Run the query: LIST CLIENTS NAME COMPANY ADDRESS SAMPLE 3 TOXML
map["ADDRESS_MV":[map["ADDRESS":"45, reu de Rivoli"]] "COMPANY":"Chez Paul" "NAME":"Paul Castiglione" "_ID":"9999"]
map["ADDRESS_MV":[map["ADDRESS":"854, reu de Rivoli"]] "COMPANY":"Otis Concrete" "NAME":"Fredrick Anderson" "_ID":"10034"]
map["ADDRESS_MV":[map["ADDRESS":"7925 S. Blake St."]] "COMPANY":"Riley Architects" "NAME":"Beverly Ostrovich" "_ID":"9980"]

=== Demo Query Batched ===
=== Run the query:
=== &udt.QueryConfig{Select:[]string{"SELECT ORDERS WITH ORD_DATE=\"10/25/2000\""}, File:"ORDERS", Fields:[]string{"ID", "ORD_DATE", "ORD_TIME"}, BatchSize:25}
map["ID":"890" "ORD_DATE":"10/25/2000" "ORD_TIME":"07:00PM" "_ID":"890"]
map["ID":"884" "ORD_DATE":"10/25/2000" "ORD_TIME":"11:45PM" "_ID":"884"]
map["ID":"881" "ORD_DATE":"10/25/2000" "ORD_TIME":"05:00PM" "_ID":"881"]
map["ID":"883" "ORD_DATE":"10/25/2000" "ORD_TIME":"12:34PM" "_ID":"883"]
map["ID":"874" "ORD_DATE":"10/25/2000" "ORD_TIME":"05:56PM" "_ID":"874"]
```
