package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	valuesGroupByKey := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		reduceFileName := reduceName(jobName, i, reduceTaskNumber)

		reduceFile, err := os.OpenFile(reduceFileName, os.O_RDONLY, 0644)
		if err != nil {
			log.Fatalln(err)
		}

		decoder := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			valuesGroupByKey[kv.Key] = append(valuesGroupByKey[kv.Key], kv.Value)
		}
	}

	file, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln(err)
	}

	enc := json.NewEncoder(file)
	for key, values := range valuesGroupByKey {
		enc.Encode(KeyValue{key, reduceF(key, values)})
	}
	file.Close()
}
