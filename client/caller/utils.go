package caller

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// GetCallerID returns the name of the currently running process
func GetCallerID() ID {
	return ID(filepath.Base(os.Args[0]))
}

// GetComponent returns the package path of the calling function
// The argument i is the number of stack frames to ascend.
func GetComponent(i int) Component {
	// Get the program counter for the calling function
	pc, _, _, ok := runtime.Caller(i + 1)
	if !ok {
		return "unknown"
	}

	// Retrieve the full function name, including the package path
	fullFuncName := runtime.FuncForPC(pc).Name()

	// Separates the package and function
	lastSlash := strings.LastIndex(fullFuncName, ".")

	// Extract the package name
	return Component(fullFuncName[:lastSlash])
}
