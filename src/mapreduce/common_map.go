package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
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
	content, err := ioutil.ReadFile(inFile)

	if err != nil {
		log.Fatal(err)
	}

	keyValues := mapF(inFile, string(content))
	reduceFiles := make(map[string]*os.File)

	for _, kv := range keyValues {
		reduceTaskNumber := ihash(kv.Key) % nReduce

		if err != nil {
			log.Fatal(err)
		}

		reduceFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)

		if reduceFiles[reduceFileName] == nil {
			f, err := os.OpenFile(reduceFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			reduceFiles[reduceFileName] = f
		}

		f := reduceFiles[reduceFileName]
		enc := json.NewEncoder(f)
		enc.Encode(&kv)
	}

	for _, f := range reduceFiles {
		f.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
