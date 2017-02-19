package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"os"
	"log"
	"encoding/json"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//
	// You will need to write this function.
	//
	// The intermediate output of a map task is stored as multiple
	// files, one per destination reduce task. The file name includes
	// both the map task number and the reduce task number. Use the
	// filename generated by reduceName(jobName, mapTaskNumber, r) as
	// the intermediate file for reduce task r. Call ihash() (see below)
	// on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//

	// firstly, i need get the whole content and standardize them.
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("map reading file error: " + err.Error())
	}

	// then, use mapF given to get all key-value pairs.
	mapOut := mapF(inFile, string(content))

	// finally, we need to split all key-values into different reduce files,
	// using json to store each file.
	interkv := make([]map[string][]string, nReduce)
	//interkv := make([]map[string]string, nReduce)
	for i:=0; i<nReduce; i++ {
		interkv[i] = make(map[string][]string)
	}

	for _, kv := range mapOut {
		pos := ihash(kv.Key) % nReduce
		_, ok := interkv[pos][kv.Key]

		if !ok {
			interkv[pos][kv.Key] = []string{kv.Value}
			//interkv[pos][kv.Key] = kv.Value
		} else {
			interkv[pos][kv.Key] = append(interkv[pos][kv.Key], kv.Value)
			//log.Fatal("mapper duplicate key in a file error: " + inFile + " " + kv.Key)
		}
	}

	storeInto(&interkv, jobName, mapTaskNumber)

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// inner function to store all kv into different files.
// since it's interfile, a single k-v is wrapped into k-[v].
// It makes it more convenient to deal with k-[v1,v2,...]
func storeInto(interkv *[]map[string][]string,
				jobName string, mapTaskNumber int) {
	writein := func(filename string, interkv *map[string][]string){
		file, err := os.Create(filename); ErrorClient(err); defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.Encode(interkv)
	}
	for which, kv := range *interkv {
		filename := reduceName(jobName, mapTaskNumber, which)
		if exists(filename) {
			// it means other mapper has created this file
			// so we need load it into memory first, then delete the old file and create a new one.
			//var tmp map[string][]string
			var tmp map[string][]string
			content, err := ioutil.ReadFile(filename); ErrorClient(err)
			err = json.Unmarshal(content, &tmp); ErrorClient(err)
			for k, v := range tmp {
				if _, ok := kv[k]; !ok {
					kv[k] = v
				} else {
					for _, word := range v {
						kv[k] = append(kv[k], word)
					}
					//log.Fatal("mapper duplicate key in multi file error:" + filename + " " + k)
				}
			}
		}
		writein(filename, &kv)
	}
}