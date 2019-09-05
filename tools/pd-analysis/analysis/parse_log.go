package analysis

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
)

// Interpreter is the interface for all analysis to parse log
type Interpreter interface {
	ParseLog(filename string)
}

// ParseLog is to parse log for transfer region counter.
func (TransferRegionCount) ParseLog(filename string) {
	r, _ := regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?mv peer: store ([0-9]*) to ([0-9]*).*?")
	// Open file
	fi, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()
	br := bufio.NewReader(fi)
	for {
		// Read content
		content, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error: %s\n", err)
			return
		}
		// Regex for each line
		result := r.FindStringSubmatch(string(content))
		if len(result) == 0 {
			continue
		} else if len(result) == 4 {
			// Convert result
			resultUint64 := make([]uint64, 0)
			for i := 1; i < 4; i++ {
				num, err := strconv.ParseInt(result[i], 10, 64)
				if err != nil {
					fmt.Printf("Error: %s\n", err)
					return
				}
				resultUint64 = append(resultUint64, uint64(num))
			}
			regionID, sourceID, targetID := resultUint64[0], resultUint64[1], resultUint64[2]
			// Push edge
			GetTransferRegionCounter().AddTarget(regionID, targetID)
			GetTransferRegionCounter().AddSource(regionID, sourceID)

		} else {
			log.Fatal("Parse Log Error, with", content)
		}
	}
}
