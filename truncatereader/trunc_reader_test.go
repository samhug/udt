package truncatereader

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
)

func TestNewTruncReader(t *testing.T) {

	type _testCase struct {
		readSize int // The number of bytes to read at a time
		in       string
		out      string
		pattern  string
	}

	testCases := []_testCase{

		// Test single Read call
		{8, "abcdef", "abcdef", "xy"},      // 0 - no match
		{8, "abcdef", "", "abcdef"},        // 1 - full match
		{8, "abcdef", "abcdef", "ab"},      // 2 - match beginning
		{8, "abcdef", "abcdef", "cd"},      // 3 - match middle
		{8, "abcdef", "abcd", "ef"},        // 4 - match end
		{8, "abcdef", "abcdef", "dq"},      // 5 - partial match
		{8, "abcdef", "abcdef", "efg"},     // 6 - partial match at end
		{8, "abcdef", "abcdef", "abcdefg"}, // 7 - full partial match
		{8, "abcddf", "abcd", "df"},        // 9 - partial match /w backtrack

		// Test multiple Read calls
		{4, "abcdef", "", "abcdef"},         // 10  - full match
		{4, "abcdef", "abcdef", "cd"},       // 11  - match end of first read
		{4, "abcdefgh", "abcd", "efgh"},     // 12 - match second read
		{4, "abcdef", "ab", "cdef"},         // 13 - match split between reads
		{4, "00aaab", "00a", "aab"},         // 14 - match split between reads /w backtrack
		{4, "000aaaaaab", "000a", "aaaaab"}, // 15 - match split between reads /w backtrack
	}

	for i, testCase := range testCases {
		r := NewTruncReader(strings.NewReader(testCase.in), []byte(testCase.pattern))

		out, err := readAll(r, testCase.readSize)
		if err != nil {
			t.Error(fmt.Sprintf("testCases[%d]", i), err)
			continue
		}

		assertEqual(t, []byte(testCase.out), out, fmt.Sprintf("testCases[%d], input: %q, pattern: %q", i, testCase.in, testCase.pattern))
	}
}

func benchHelper(b *testing.B, N int64, patternSize int) {
	b.Helper()

	r := rand.New(rand.NewSource(0))
	searchPattern := make([]byte, patternSize)
	if _, err := r.Read(searchPattern); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := ioutil.ReadAll(NewTruncReader(io.LimitReader(r, N), searchPattern))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTruncReader64(b *testing.B) {
	benchHelper(b, 10000, 64)
}

func BenchmarkTruncReader512(b *testing.B) {
	benchHelper(b, 10000, 512)
}

func BenchmarkTruncReader1024(b *testing.B) {
	benchHelper(b, 10000, 1024)
}

func BenchmarkPipeReader(b *testing.B) {
	var N int64 = 10000

	r := rand.New(rand.NewSource(0))

	for n := 0; n < b.N; n++ {
		_, err := ioutil.ReadAll(io.LimitReader(r, N))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func readAll(r io.Reader, chunkSize int) ([]byte, error) {

	var output []byte

	for {
		buf := make([]byte, chunkSize)
		n, err := r.Read(buf)
		output = append(output, buf[:n]...)
		if err != nil {
			if err == io.EOF {
				break
			}
			return output, err
		}
	}

	return output, nil
}

func assertEqual(t *testing.T, expected []byte, received []byte, failMsg string) {
	t.Helper()
	if !bytes.Equal(expected, received) {
		t.Fatalf("Unexpected value: %s\nExpected:\n%q\nReceived:\n%q\n", failMsg, expected, received)
	}
}
