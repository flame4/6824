package mapreduce

import (
	"fmt"
	"strconv"
	"os"
	"log"
)

// Debugging enabled?
const debugEnabled = true

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// exists indicate whether file is exist or not.
func exists(file string) bool {
	_, err := os.Stat(file)
	return os.IsNotExist(err)
}

// A error client to hold and deal with all error.
func ErrorClient(err error) bool{
	return Error(err)
}

func Error(err error) bool{
	if err != nil {
		log.Fatal(err)
		return false
	}
	return true
}