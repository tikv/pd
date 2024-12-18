// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/tikv/pd/tools/pd-ut/alloc"

	// Set the correct value when it runs inside docker.
	_ "go.uber.org/automaxprocs"
)

func usage() bool {
	msg := `// run all tests
pd-ut

// show usage
pd-ut -h

// list all packages
pd-ut list

// list test cases of a single package
pd-ut list $package

// list test cases that match a pattern
pd-ut list $package 'r:$regex'

// run all tests
pd-ut run

// run test all cases of a single package
pd-ut run $package

// run test all cases of a set of packages
pd-ut run $package1,$package2,...

// run test cases of a single package
pd-ut run $package $test

// run test cases that match a pattern
pd-ut run $package 'r:$regex'

// build all test package
pd-ut build

// build a test package
pd-ut build xxx

// write the junitfile
pd-ut run --junitfile xxx

// test with race flag
pd-ut run --race

// test with coverprofile
pd-ut run --coverprofile xxx
go tool cover --func=xxx`

	fmt.Println(msg)
	return true
}

var (
	modulePath           = filepath.Join("github.com", "tikv", "pd")
	integrationsTestPath = filepath.Join("tests", "integrations")
)

var (
	// runtime
	parallel         int
	workDir          string
	coverFileTempDir string
	// arguments
	race         bool
	junitFile    string
	coverProfile string
	ignoreDirs   string
	cache        bool
)

func main() {
	race = handleFlag("--race")
	parallelStr := stripFlag("--parallel")
	junitFile = stripFlag("--junitfile")
	coverProfile = stripFlag("--coverprofile")
	ignoreDirs = stripFlag("--ignore")
	cache = handleFlag("--cache")

	if coverProfile != "" {
		var err error
		coverFileTempDir, err = os.MkdirTemp("", "cov")
		if err != nil {
			fmt.Println("create temp dir fail", coverFileTempDir)
			os.Exit(1)
		}
		defer os.RemoveAll(coverFileTempDir)
	}

	var err error
	procs := runtime.GOMAXPROCS(0)
	if parallelStr == "" {
		// Get the correct count of CPU if it's in docker.
		parallel = procs
	} else {
		parallel, err = strconv.Atoi(parallelStr)
		if err != nil {
			fmt.Println("parse parallel error", err)
			return
		}
		if parallel > procs {
			fmt.Printf("Recommend to set parallel be same as the GOMAXPROCS=%d\n", procs)
		}
	}
	workDir, err = os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd() error", err)
	}

	srv := alloc.RunHTTPServer()
	defer func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Fatal("server shutdown error", zap.Error(err))
		}
	}()

	var isSucceed bool
	// run all tests
	if len(os.Args) == 1 {
		isSucceed = cmdRun()
	}

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "list":
			isSucceed = cmdList(os.Args[2:]...)
		case "build":
			isSucceed = cmdBuild(os.Args[2:]...)
		case "run":
			isSucceed = cmdRun(os.Args[2:]...)
		case "it":
			// run integration tests
			if len(os.Args) >= 3 {
				modulePath = filepath.Join(modulePath, integrationsTestPath)
				workDir = filepath.Join(workDir, integrationsTestPath)
				switch os.Args[2] {
				case "run":
					isSucceed = cmdRun(os.Args[3:]...)
				case "list":
					isSucceed = cmdList(os.Args[3:]...)
				default:
					isSucceed = usage()
				}
			}
		default:
			isSucceed = usage()
		}
	}
	if !isSucceed {
		os.Exit(1)
	}
}

func cmdList(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		log.Println("list package error", err)
		return false
	}

	// list all packages
	if len(args) == 0 {
		for _, pkg := range pkgs {
			fmt.Println(pkg)
		}
		return false
	}

	// list test case of a single package
	if len(args) == 1 || len(args) == 2 {
		pkg := args[0]
		pkgs = filter(pkgs, func(s string) bool { return s == pkg })
		if len(pkgs) != 1 {
			fmt.Println("package not exist", pkg)
			return false
		}

		err := buildTestBinary(pkg)
		if err != nil {
			log.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			log.Println("check test binary existence error", err)
			return false
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}

		res := listTestCases(pkg, nil)

		if len(args) == 2 {
			res, err = filterTestCases(res, args[1])
			if err != nil {
				log.Println("filter test cases error", err)
				return false
			}
		}

		for _, x := range res {
			fmt.Println(x.test)
		}
	}
	return true
}

func cmdBuild(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		log.Println("list package error", err)
		return false
	}

	// build all packages
	if len(args) == 0 {
		_, err := buildTestBinaryMulti(pkgs)
		if err != nil {
			fmt.Println("build package error", pkgs, err)
			return false
		}
		return true
	}

	// build test binary of a single package
	if len(args) >= 1 {
		var dirPkgs []string
		for _, pkg := range pkgs {
			if strings.Contains(pkg, args[0]) {
				dirPkgs = append(dirPkgs, pkg)
			}
		}

		_, err := buildTestBinaryMulti(dirPkgs)
		if err != nil {
			log.Println("build package error", dirPkgs, err)
			return false
		}
	}
	return true
}

func cmdRun(args ...string) bool {
	var err error
	pkgs, err := listPackages()
	if err != nil {
		fmt.Println("list packages error", err)
		return false
	}
	tasks := make([]task, 0, 5000)
	start := time.Now()
	// run all tests
	if len(args) == 0 {
		tasks, err = runExistingTestCases(pkgs)
		if err != nil {
			fmt.Println("run existing test cases error", err)
			return false
		}
	}

	// run tests for a single package
	if len(args) == 1 {
		dirs := strings.Split(args[0], ",")
		var dirPkgs []string
		for _, pkg := range pkgs {
			for _, dir := range dirs {
				if strings.Contains(pkg, dir) {
					dirPkgs = append(dirPkgs, pkg)
				}
			}
		}

		tasks, err = runExistingTestCases(dirPkgs)
		if err != nil {
			fmt.Println("run existing test cases error", err)
			return false
		}
	}

	// run a single test
	if len(args) == 2 {
		pkg := args[0]
		err := buildTestBinary(pkg)
		if err != nil {
			log.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			log.Println("check test binary existence error", err)
			return false
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}

		tasks = listTestCases(pkg, tasks)
		tasks, err = filterTestCases(tasks, args[1])
		if err != nil {
			log.Println("filter test cases error", err)
			return false
		}
	}

	fmt.Printf("building task finish, parallelism=%d, count=%d, takes=%v\n", parallel*2, len(tasks), time.Since(start))

	taskCh := make(chan task, 100)
	works := make([]numa, parallel)
	var wg sync.WaitGroup
	for i := range parallel {
		wg.Add(1)
		go works[i].worker(&wg, taskCh)
	}

	shuffle(tasks)

	start = time.Now()
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	fmt.Println("run all tasks takes", time.Since(start))

	if junitFile != "" {
		out := collectTestResults(works)
		f, err := os.Create(junitFile)
		if err != nil {
			fmt.Println("create junit file fail:", err)
			return false
		}
		if err := write(f, out); err != nil {
			fmt.Println("write junit file error:", err)
			return false
		}
	}

	if coverProfile != "" {
		collectCoverProfileFile()
	}

	for _, work := range works {
		if work.Fail {
			return false
		}
	}
	return true
}

func runExistingTestCases(pkgs []string) (tasks []task, err error) {
	// run tests for a single package
	content, err := buildTestBinaryMulti(pkgs)
	if err != nil {
		fmt.Println("build package error", pkgs, err)
		return nil, err
	}

	// read content
	existPkgs := make(map[string]struct{})
	for _, line := range strings.Split(string(content), "\n") {
		if len(line) == 0 || strings.Contains(line, "no test files") {
			continue
		}
		// format is
		// ?   	github.com/tikv/pd/server/apiv2/middlewares	[no test files]
		// ok  	github.com/tikv/pd/server/api	0.173s
		existPkgs[strings.Split(line, "\t")[1]] = struct{}{}
		fmt.Println(line)
	}

	wg := &sync.WaitGroup{}
	tasksChannel := make(chan []task, len(pkgs))
	for _, pkg := range pkgs {
		_, ok := existPkgs[filepath.Join(modulePath, pkg)]
		if !ok {
			fmt.Println("no test case in ", pkg)
			continue
		}

		wg.Add(1)
		go listTestCasesConcurrent(wg, pkg, tasksChannel)
	}

	wg.Wait()
	close(tasksChannel)
	for t := range tasksChannel {
		tasks = append(tasks, t...)
	}
	return tasks, nil
}

// stripFlag strip the '--flag xxx' from the command line os.Args
// Example of the os.Args changes
// Before: ut run pkg TestXXX --coverprofile xxx --junitfile yyy --parallel 16
// After: ut run pkg TestXXX
// The value of the flag is returned.
func stripFlag(flag string) string {
	var res string
	tmp := os.Args[:0]
	// Iter to the flag
	var i int
	for ; i < len(os.Args); i++ {
		if os.Args[i] == flag {
			i++
			break
		}
		tmp = append(tmp, os.Args[i])
	}
	// Handle the flag
	if i < len(os.Args) {
		res = os.Args[i]
		i++
	}
	// Iter the remain flags
	for ; i < len(os.Args); i++ {
		tmp = append(tmp, os.Args[i])
	}

	os.Args = tmp
	return res
}

func handleFlag(f string) (found bool) {
	tmp := os.Args[:0]
	for i := range os.Args {
		if os.Args[i] == f {
			found = true
			continue
		}
		tmp = append(tmp, os.Args[i])
	}
	os.Args = tmp
	return
}

type task struct {
	pkg  string
	test string
}

func (t *task) String() string {
	return t.pkg + " " + t.test
}

func listTestCases(pkg string, tasks []task) []task {
	newCases := listNewTestCases(pkg)
	for _, c := range newCases {
		tasks = append(tasks, task{pkg, c})
	}

	return tasks
}

func listTestCasesConcurrent(wg *sync.WaitGroup, pkg string, tasksChannel chan<- []task) {
	defer wg.Done()
	newCases := listNewTestCases(pkg)
	var tasks []task
	for _, c := range newCases {
		tasks = append(tasks, task{pkg, c})
	}
	tasksChannel <- tasks
}

func filterTestCases(tasks []task, arg1 string) ([]task, error) {
	if strings.HasPrefix(arg1, "r:") {
		r, err := regexp.Compile(arg1[2:])
		if err != nil {
			return nil, err
		}
		tmp := tasks[:0]
		for _, task := range tasks {
			if r.MatchString(task.test) {
				tmp = append(tmp, task)
			}
		}
		return tmp, nil
	}
	tmp := tasks[:0]
	for _, task := range tasks {
		if strings.Contains(task.test, arg1) {
			tmp = append(tmp, task)
		}
	}
	return tmp, nil
}

func listPackages() ([]string, error) {
	listPath := strings.Join([]string{".", "..."}, string(filepath.Separator))
	cmd := exec.Command("go", "list", listPath)
	cmd.Dir = workDir
	ss, err := cmdToLines(cmd)
	if err != nil {
		return nil, withTrace(err)
	}

	ret := ss[:0]
	for _, s := range ss {
		if !strings.HasPrefix(s, modulePath) {
			continue
		}
		pkg := s[len(modulePath)+1:]
		if skipDIR(pkg) {
			continue
		}
		ret = append(ret, pkg)
	}
	return ret, nil
}

type numa struct {
	Fail    bool
	results []testResult
}

func (n *numa) worker(wg *sync.WaitGroup, ch chan task) {
	defer wg.Done()
	for t := range ch {
		res := n.runTestCase(t.pkg, t.test)
		if res.Failure != nil {
			fmt.Println("[FAIL] ", t.pkg, t.test)
			fmt.Fprintf(os.Stderr, "err=%s\n%s", res.err.Error(), res.Failure.Contents)
			n.Fail = true
		}
		n.results = append(n.results, res)
	}
}

type testResult struct {
	JUnitTestCase
	d   time.Duration
	err error
}

func (n *numa) runTestCase(pkg string, fn string) testResult {
	res := testResult{
		JUnitTestCase: JUnitTestCase{
			ClassName: filepath.Join(modulePath, pkg),
			Name:      fn,
		},
	}

	var buf bytes.Buffer
	var err error
	var start time.Time
	for range 3 {
		cmd := n.testCommand(pkg, fn)
		cmd.Dir = filepath.Join(workDir, pkg)
		// Combine the test case output, so the run result for failed cases can be displayed.
		cmd.Stdout = &buf
		cmd.Stderr = &buf

		start = time.Now()
		err = cmd.Run()
		if err != nil {
			var exitError *exec.ExitError
			if errors.As(err, &exitError) {
				// Retry 3 times to get rid of the weird error:
				switch err.Error() {
				case "signal: segmentation fault (core dumped)":
					buf.Reset()
					continue
				case "signal: trace/breakpoint trap (core dumped)":
					buf.Reset()
					continue
				}
				if strings.Contains(buf.String(), "panic during panic") {
					buf.Reset()
					continue
				}
			}
		}
		break
	}
	if err != nil {
		res.Failure = &JUnitFailure{
			Message:  "Failed",
			Contents: buf.String(),
		}
		res.err = err
	}

	res.d = time.Since(start)
	res.Time = formatDurationAsSeconds(res.d)
	return res
}

func collectTestResults(workers []numa) JUnitTestSuites {
	version := goVersion()
	// pkg => test cases
	pkgs := make(map[string][]JUnitTestCase)
	durations := make(map[string]time.Duration)

	// The test result in workers are shuffled, so group by the packages here
	for _, n := range workers {
		for _, res := range n.results {
			cases, ok := pkgs[res.ClassName]
			if !ok {
				cases = make([]JUnitTestCase, 0, 10)
			}
			cases = append(cases, res.JUnitTestCase)
			pkgs[res.ClassName] = cases
			durations[res.ClassName] += res.d
		}
	}

	suites := JUnitTestSuites{}
	// Turn every package result to a suite.
	for pkg, cases := range pkgs {
		suite := JUnitTestSuite{
			Tests:      len(cases),
			Failures:   failureCases(cases),
			Time:       formatDurationAsSeconds(durations[pkg]),
			Name:       pkg,
			Properties: packageProperties(version),
			TestCases:  cases,
		}
		suites.Suites = append(suites.Suites, suite)
	}
	return suites
}

func failureCases(input []JUnitTestCase) int {
	sum := 0
	for _, v := range input {
		if v.Failure != nil {
			sum++
		}
	}
	return sum
}

func (*numa) testCommand(pkg string, fn string) *exec.Cmd {
	args := make([]string, 0, 10)
	// let the test run in the verbose mode.
	args = append(args, "-test.v")
	exe := strings.Join([]string{".", testFileName(pkg)}, string(filepath.Separator))
	if coverProfile != "" {
		fileName := strings.ReplaceAll(pkg, string(filepath.Separator), "_") + "." + fn
		tmpFile := filepath.Join(coverFileTempDir, fileName)
		args = append(args, "-test.coverprofile", tmpFile)
	}
	if strings.Contains(fn, "Suite") {
		args = append(args, "-test.cpu", fmt.Sprint(parallel/2))
	} else {
		args = append(args, "-test.cpu", "1")
	}
	if !race {
		args = append(args, []string{"-test.timeout", "2m"}...)
	} else {
		// it takes a longer when race is enabled. so it is set more timeout value.
		args = append(args, []string{"-test.timeout", "5m"}...)
	}

	// core.test -test.run TestClusteredPrefixColum
	args = append(args, "-test.run", "^"+fn+"$")

	return exec.Command(exe, args...)
}

func skipDIR(pkg string) bool {
	skipDir := []string{"bin", "cmd", "realcluster"}
	if ignoreDirs != "" {
		dirs := strings.Split(ignoreDirs, ",")
		skipDir = append(skipDir, dirs...)
	}
	for _, ignore := range skipDir {
		if strings.HasPrefix(pkg, ignore) {
			return true
		}
	}
	return false
}

func generateBuildCache() error {
	if !cache {
		return nil
	}
	fmt.Println("generate build cache")
	// cd cmd/pd-server && go test -tags=deadlock -exec-=true -vet=off -toolexec=go-compile-without-link
	cmd := exec.Command("go", "test", "-exec=true", "-vet", "off", "--tags=deadlock")
	goCompileWithoutLink := fmt.Sprintf("-toolexec=%s", filepath.Join(workDir, "tools", "pd-ut", "go-compile-without-link.sh"))
	cmd.Dir = filepath.Join(workDir, "cmd", "pd-server")
	if strings.Contains(workDir, integrationsTestPath) {
		cmd.Dir = filepath.Join(workDir[:strings.LastIndex(workDir, integrationsTestPath)], "cmd", "pd-server")
		goCompileWithoutLink = fmt.Sprintf("-toolexec=%s", filepath.Join(workDir[:strings.LastIndex(workDir, integrationsTestPath)],
			"tools", "pd-ut", "go-compile-without-link.sh"))
	}
	cmd.Args = append(cmd.Args, goCompileWithoutLink)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

// buildTestBinaryMulti is much faster than build the test packages one by one.
func buildTestBinaryMulti(pkgs []string) ([]byte, error) {
	// staged build, generate the build cache for all the tests first, then generate the test binary.
	// This way is faster than generating test binaries directly, because the cache can be used.
	if err := generateBuildCache(); err != nil {
		return nil, withTrace(err)
	}

	// go test --exec=xprog --tags=deadlock -vet=off --count=0 $(pkgs)
	// workPath just like `/pd/tests/integrations`
	xprogPath := filepath.Join(workDir, "bin", "xprog")
	if strings.Contains(workDir, integrationsTestPath) {
		xprogPath = filepath.Join(workDir[:strings.LastIndex(workDir, integrationsTestPath)], "bin", "xprog")
	}
	packages := make([]string, 0, len(pkgs))
	for _, pkg := range pkgs {
		packages = append(packages, filepath.Join(modulePath, pkg))
	}

	// We use 2 * parallel for `go build` to make it faster.
	p := strconv.Itoa(parallel * 2)
	cmd := exec.Command("go", "test", "-p", p, "--exec", xprogPath, "-vet", "off", "--tags=deadlock")
	if coverProfile != "" {
		coverPkg := strings.Join([]string{".", "..."}, string(filepath.Separator))
		if strings.Contains(workDir, integrationsTestPath) {
			coverPkg = filepath.Join("..", "..", "...")
		}
		cmd.Args = append(cmd.Args, "-cover", fmt.Sprintf("-coverpkg=%s", coverPkg))
	}
	cmd.Args = append(cmd.Args, packages...)
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	cmd.Dir = workDir
	outputFile, err := os.CreateTemp("", "pd_tests")
	if err != nil {
		return nil, err
	}
	cmd.Stdout = outputFile
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, withTrace(err)
	}

	// return content
	content, err := os.ReadFile(outputFile.Name())
	if err != nil {
		return nil, err
	}
	defer outputFile.Close()

	return content, nil
}

func buildTestBinary(pkg string) error {
	//nolint:gosec
	cmd := exec.Command("go", "test", "-c", "-vet", "off", "--tags=deadlock", "-o", testFileName(pkg), "-v")
	if coverProfile != "" {
		coverPkg := strings.Join([]string{".", "..."}, string(filepath.Separator))
		cmd.Args = append(cmd.Args, "-cover", fmt.Sprintf("-coverpkg=%s", coverPkg))
	}
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	cmd.Dir = filepath.Join(workDir, pkg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

func testBinaryExist(pkg string) (bool, error) {
	_, err := os.Stat(testFileFullPath(pkg))
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			return false, nil
		}
	}
	return true, withTrace(err)
}

func testFileName(pkg string) string {
	_, file := filepath.Split(pkg)
	return file + ".test.bin"
}

func testFileFullPath(pkg string) string {
	return filepath.Join(workDir, pkg, testFileName(pkg))
}

func listNewTestCases(pkg string) []string {
	exe := strings.Join([]string{".", testFileName(pkg)}, string(filepath.Separator))
	// core.test -test.list Test
	cmd := exec.Command(exe, "-test.list", "Test")
	cmd.Dir = filepath.Join(workDir, pkg)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	err := cmd.Run()
	res := strings.Split(buf.String(), "\n")
	if err != nil && len(res) == 0 {
		fmt.Println("err ==", err)
	}
	return filter(res, func(s string) bool {
		return strings.HasPrefix(s, "Test") && s != "TestT" && s != "TestBenchDaily"
	})
}

func cmdToLines(cmd *exec.Cmd) ([]string, error) {
	res, err := cmd.Output()
	if err != nil {
		return nil, withTrace(err)
	}
	ss := bytes.Split(res, []byte{'\n'})
	ret := make([]string, len(ss))
	for i, s := range ss {
		ret[i] = string(s)
	}
	return ret, nil
}

func filter(input []string, f func(string) bool) []string {
	ret := input[:0]
	for _, s := range input {
		if f(s) {
			ret = append(ret, s)
		}
	}
	return ret
}

func shuffle(tasks []task) {
	for i := 0; i < len(tasks); i++ {
		pos := rand.Intn(len(tasks))
		tasks[i], tasks[pos] = tasks[pos], tasks[i]
	}
}

type errWithStack struct {
	err error
	buf []byte
}

func (e *errWithStack) Error() string {
	return e.err.Error() + "\n" + string(e.buf)
}

func withTrace(err error) error {
	if err == nil {
		return err
	}
	var errStack *errWithStack
	if errors.As(err, &errStack) {
		return err
	}
	var stack [4096]byte
	sz := runtime.Stack(stack[:], false)
	return &errWithStack{err, stack[:sz]}
}

func formatDurationAsSeconds(d time.Duration) string {
	return fmt.Sprintf("%f", d.Seconds())
}

func packageProperties(goVersion string) []JUnitProperty {
	return []JUnitProperty{
		{Name: "go.version", Value: goVersion},
	}
}

// goVersion returns the version as reported by the go binary in PATH. This
// version will not be the same as runtime.Version, which is always the version
// of go used to build the gotestsum binary.
//
// To skip the os/exec call set the GOVERSION environment variable to the
// desired value.
func goVersion() string {
	if version, ok := os.LookupEnv("GOVERSION"); ok {
		return version
	}
	cmd := exec.Command("go", "version")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimPrefix(strings.TrimSpace(string(out)), "go version ")
}

func write(out io.Writer, suites JUnitTestSuites) error {
	doc, err := xml.MarshalIndent(suites, "", "\t")
	if err != nil {
		return err
	}
	_, err = out.Write([]byte(xml.Header))
	if err != nil {
		return err
	}
	_, err = out.Write(doc)
	return err
}

// JUnitTestSuites is a collection of JUnit test suites.
type JUnitTestSuites struct {
	XMLName xml.Name `xml:"testsuites"`
	Suites  []JUnitTestSuite
}

// JUnitTestSuite is a single JUnit test suite which may contain many
// testcases.
type JUnitTestSuite struct {
	XMLName    xml.Name        `xml:"testsuite"`
	Tests      int             `xml:"tests,attr"`
	Failures   int             `xml:"failures,attr"`
	Time       string          `xml:"time,attr"`
	Name       string          `xml:"name,attr"`
	Properties []JUnitProperty `xml:"properties>property,omitempty"`
	TestCases  []JUnitTestCase
}

// JUnitTestCase is a single test case with its result.
type JUnitTestCase struct {
	XMLName     xml.Name          `xml:"testcase"`
	ClassName   string            `xml:"classname,attr"`
	Name        string            `xml:"name,attr"`
	Time        string            `xml:"time,attr"`
	SkipMessage *JUnitSkipMessage `xml:"skipped,omitempty"`
	Failure     *JUnitFailure     `xml:"failure,omitempty"`
}

// JUnitSkipMessage contains the reason why a testcase was skipped.
type JUnitSkipMessage struct {
	Message string `xml:"message,attr"`
}

// JUnitProperty represents a key/value pair used to define properties.
type JUnitProperty struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// JUnitFailure contains data related to a failed test.
type JUnitFailure struct {
	Message  string `xml:"message,attr"`
	Type     string `xml:"type,attr"`
	Contents string `xml:",chardata"`
}
