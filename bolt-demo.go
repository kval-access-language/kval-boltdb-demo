package main

import (
	"fmt"
	kval "github.com/kval-access-language/kval-boltdb"
	"os"
)

func main() {

	kb, err := kval.Connect("newdb.bolt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bolt database: %#v", err)
		os.Exit(1)
	}
	defer kval.Disconnect(kb)

	//Lets do a test insert...
	res, _ := kval.Query(kb, "INS test bucket one >> test bucket two >>>> key one :: value one")
	res, _ = kval.Query(kb, "INS test bucket one >> test bucket two >>>> key two :: value two")
	res, _ = kval.Query(kb, "INS test bucket one >>>> key three :: value three")

	//list query to see if any of our data exists
	lis, _ := kval.Query(kb, "LIS test bucket one")

	//And output some stats from BotlDB's structs...
	fmt.Printf("Bucket two keys: %d\n", res.Stats.KeyN)
	fmt.Printf("Bucket onw depth: %d\n", lis.Stats.Depth)

	//retrieve some data?
	get, _ := kval.Query(kb, "GET test bucket one >> test bucket two")

	//we should have two results, so may need a loop to access...
	fmt.Printf("How many results in GET: %d\n", len(get.Result))

	for k, v := range get.Result {
		fmt.Printf("Key: %s, Value: %s\n", k, v)
	}

	//binary objects?
	_ = kval.StoreBlob(kb, "INS binary data >>>> data one", "text/plain", []byte("some data"))

	//get it back?
	data, _ := kval.Query(kb, "GET binary data >>>> data one")

	//UnwrapBlob and GetblobData are helpers to access the data, but let's just print it
	fmt.Printf("Encoded data: %s\n", data.Result["data one"])

	//How about renaming our data?
	kval.Query(kb, "REN test bucket one >> test bucket two => new name for bucket two")

	//repeat how we retrieved the data last time...
	get, _ = kval.Query(kb, "GET test bucket one >> new name for bucket two")

	//we should have two results, so may need a loop to access...
	fmt.Printf("How many results in GET: %d\n", len(get.Result))

	for k, v := range get.Result {
		fmt.Printf("Key: %s, Value: %s\n", k, v)
	}

	//check our old bucket has gone...
	lis, _ = kval.Query(kb, "LIS test bucket one >> test bucket two")

	fmt.Printf("Bucket still exists? %v\n", lis.Exists)

	//delete is easy too...
	del, _ := kval.Query(kb, "DEL test bucket one")

	//any data left?
	fmt.Printf("Bucket keys:  %d\n", del.Stats.KeyN)
	fmt.Printf("Bucket depth: %d\n", del.Stats.Depth)

	if del.Stats.KeyN == 0 && del.Stats.Depth == 0 {
		fmt.Println("Thanks for checking out this demo Gophers!")
	}
}
