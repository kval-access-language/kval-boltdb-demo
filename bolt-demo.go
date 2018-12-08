package main

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	kval "github.com/kval-access-language/kval-bbolt"
	"os"
	"time"
)

func pauseAndFormat(longPause bool) {
	duration := time.Duration(2)
	if longPause {
		duration = time.Duration(4)
	}
	unit := time.Second
	fmt.Println()
	time.Sleep(duration * unit)
}

func main() {

	fmt.Println("KVAL-bbolt binding demo with a little insight into the KVAL-Access-Language for Key-store databases")
	fmt.Println("2018-12-08, by Ross Spencer")
	pauseAndFormat(false)

	kb, err := kval.Connect("kval-bboltdemo.bolt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bolt database: %#v", err)
		os.Exit(1)
	}
	defer kval.Disconnect(kb)

	//vLets do a test insert...
	fmt.Println("Create two buckets with one key-value-pair in the second bucket: \"INS bucket one >> bucket two >>>> key 1 :: value 1\"")
	_, _ = kval.Query(kb, "INS bucket one >> bucket two >>>> key 1 :: value 1")
	pauseAndFormat(false)

	fmt.Println("Add a second key-value-pair to bucket two: \"INS bucket one >> bucket two >>>> key 2 :: value 2\"")
	_, _ = kval.Query(kb, "INS bucket one >> bucket two >>>> key 2 :: value 2")
	pauseAndFormat(false)

	fmt.Println("Add a second-key to bucket one: \"INS bucket one >>>> key 3 :: value 3\"")
	_, _ = kval.Query(kb, "INS bucket one >>>> key 3 :: value 3")
	pauseAndFormat(false)

	// List query to see if any of our data exists
	fmt.Println("List query to test for the existence of bucket one")
	lis, _ := kval.Query(kb, "LIS bucket one")
	spew.Dump(lis)
	pauseAndFormat(true)

	// And output some stats from BotlDB's structs...
	fmt.Println("Return some statistics from our list structures: \"lis.Stats.KeyN\"")
	lis, _ = kval.Query(kb, "LIS bucket one >> bucket two")
	fmt.Printf("Keys in bucket two: %d\n", lis.Stats.KeyN)
	pauseAndFormat(false)

	// Retrieve some data
	fmt.Println("Retrieve some data: \"GET bucket one >> bucket two\"")
	get, _ := kval.Query(kb, "GET bucket one >> bucket two")

	//we should have two results, so may need a loop to access...
	fmt.Printf("How many results in GET: %d\n", len(get.Result))
	fmt.Println("-------------------------------")
	for k, v := range get.Result {
		fmt.Printf("| Key: %s | Value: %s |\n", k, v)
		fmt.Println("-------------------------------")
	}
	pauseAndFormat(false)

	//binary objects?
	fmt.Println("Insert some binary data into the store: kval.StoreBlob(kb, \"INS binary data >>>> data one\", \"text/plain\", []byte(\"some data\"))")
	_ = kval.StoreBlob(kb, "INS binary data >>>> data one", "text/plain", []byte("some data"))
	pauseAndFormat(false)

	//get it back?
	fmt.Println("Retrieve the binary encoded data: \"GET binary data >>>> data one\"")
	data, _ := kval.Query(kb, "GET binary data >>>> data one")
	fmt.Println("kval.UnwrapBlob(...) and kval.GetBlobData(...) are helpers to access the data, but let's just print it:")
	fmt.Printf("Returned data: %s\n", data.Result["data one"])
	fmt.Println("You can see how to de-construct this.")
	fmt.Println("The type is 'data' the mimetype is 'text/plain' and the data is base64 encoded.")
	pauseAndFormat(false)

	//How about renaming our data?
	fmt.Println("Rename bucket two: \"REN bucket one >> bucket two => new name for bucket two\"")
	kval.Query(kb, "REN bucket one >> bucket two => new name for bucket two")
	pauseAndFormat(false)

	fmt.Println("Add a new key-value pair to the renamed bucket: \"INS bucket one >> new name for bucket two >>>> key 4 :: value 4\"")
	kval.Query(kb, "INS bucket one >> new name for bucket two >>>> key 4 :: value 4")

	//repeat how we retrieved the data last time...
	fmt.Println("Retrieve the key-value-pairs in the newly named bucket: \"GET bucket one >> new name for bucket two\"")
	get, _ = kval.Query(kb, "GET bucket one >> new name for bucket two")

	//we should have two results, so may need a loop to access...
	fmt.Printf("How many results in GET: %d\n", len(get.Result))
	fmt.Println("-------------------------------")
	for k, v := range get.Result {
		fmt.Printf("| Key: %s | Value: %s |\n", k, v)
		fmt.Println("-------------------------------")
	}
	pauseAndFormat(false)

	//check our old bucket has gone...
	fmt.Println("Now we can show that bucket two has gone/been renamed: \"LIS bucket one >> bucket two\"")
	lis, _ = kval.Query(kb, "LIS bucket one >> bucket two")
	fmt.Printf("Bucket still exists? %v\n", lis.Exists)
	pauseAndFormat(false)

	//delete is easy too...
	fmt.Println("Deleting is easy as well: \"DEL bucket one\"")
	del, _ := kval.Query(kb, "DEL bucket one")

	//any data left?
	fmt.Println("Now bbolt's statistic capabilities will show nothing in the root bucket:")
	fmt.Printf("Keys in bucket one:  %d\n", del.Stats.KeyN)
	fmt.Printf("Bucket one depth: %d\n", del.Stats.Depth)
	pauseAndFormat(false)

	//output the kval-bbolt version
	fmt.Printf("KVAL-bbolt version: %s\n", kval.Version())
	pauseAndFormat(false)

	if del.Stats.KeyN == 0 && del.Stats.Depth == 0 {
		fmt.Println("Thank you for checking out this demo Gophers!")
	}
	pauseAndFormat(false)
}
