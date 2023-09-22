package gocql

import (
	"bytes"
	"os"
	"testing"
)

func TestFuzzBugs(t *testing.T) {
	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x8200\x00\x00\x00\x00\x100\x00\x00\x12\x00\x00\x0000000" +
			"00000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 2)

	framer.writeHeader(0, opStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	_, err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x02, 0x00, 0x00, byte(opReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 2)

	head := frameHeader{
		version: 2,
		op:      opReady,
		length:  r.Len() - 8,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}
	if head.op != opReady {
		t.Fatalf("expected to get header %v got %v", opReady, head.op)
	}
}

func TestOutFrameInfo(t *testing.T) {
	tests := map[string]struct {
		frame        frameBuilder
		expectedInfo outFrameInfo
	}{
		"query": {
			frame: &writeQueryFrame{
				statement: "SELECT * FROM mytable WHERE id=? AND x=?",
				params: queryParams{
					consistency: One,
					skipMeta:    false,
					values: []queryValues{
						{
							value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
						},
						{
							value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
						},
					},
					pageSize:              5000,
					pagingState:           nil,
					serialConsistency:     0,
					defaultTimestamp:      false,
					defaultTimestampValue: 0,
					keyspace:              "",
				},
				customPayload: nil,
			},
			expectedInfo: outFrameInfo{
				op:               opQuery,
				uncompressedSize: 81,
				compressedSize:   72,
				queryValuesSize:  30,
				queryCount:       1,
			},
		},
		"execute": {
			frame: &writeExecuteFrame{
				preparedID: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
				params: queryParams{
					values: []queryValues{
						{
							value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
						},
						{
							value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
						},
					},
				},
				customPayload: nil,
			},
			expectedInfo: outFrameInfo{
				op:               opExecute,
				compressedSize:   50,
				uncompressedSize: 51,
				queryValuesSize:  30,
				queryCount:       1,
			},
		},
		"batch": {
			frame: &writeBatchFrame{
				typ: UnloggedBatch,
				statements: []batchStatment{
					{
						preparedID: nil,
						statement:  "SELECT * FROM mytable WHERE id=? AND x=?",
						values: []queryValues{
							{
								value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
							},
							{
								value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
							},
						},
					},
					{
						preparedID: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
						statement:  "",
						values: []queryValues{
							{
								value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
							},
							{
								value: []byte{'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'},
							},
						},
					},
				},
				consistency:           One,
				serialConsistency:     0,
				defaultTimestamp:      false,
				defaultTimestampValue: 0,
				customPayload:         nil,
			},
			expectedInfo: outFrameInfo{
				op:               opBatch,
				compressedSize:   96,
				uncompressedSize: 130,
				queryValuesSize:  60,
				queryCount:       2,
			},
		},
		"options": {
			frame: &writeOptionsFrame{},
			expectedInfo: outFrameInfo{
				op:               opOptions,
				compressedSize:   0,
				uncompressedSize: 0,
				queryValuesSize:  0,
				queryCount:       0,
			},
		},
		"register": {
			frame: &writeRegisterFrame{
				events: []string{"event1", "event2"},
			},
			expectedInfo: outFrameInfo{
				op:               opRegister,
				compressedSize:   20,
				uncompressedSize: 18,
				queryValuesSize:  0,
				queryCount:       0,
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fr := newFramer(SnappyCompressor{}, 4)
			ofi, err := test.frame.buildFrame(fr, 42)
			if err != nil {
				t.Fatal(err)
			}
			if ofi.op != test.expectedInfo.op {
				t.Errorf("expected op %s, but got %s", test.expectedInfo.op.String(), ofi.op.String())
			}
			if ofi.queryCount != test.expectedInfo.queryCount {
				t.Errorf("expected queryCount %d, but got %d", test.expectedInfo.queryCount, ofi.queryCount)
			}
			if ofi.queryValuesSize != test.expectedInfo.queryValuesSize {
				t.Errorf("expected queryValuesSize %d, but got %d", test.expectedInfo.queryValuesSize, ofi.queryValuesSize)
			}
			if ofi.uncompressedSize != test.expectedInfo.uncompressedSize {
				t.Errorf("expected uncompressedSize %d, but got %d", test.expectedInfo.uncompressedSize, ofi.uncompressedSize)
			}
			if ofi.compressedSize != test.expectedInfo.compressedSize {
				t.Errorf("expected compressedSize %d, but got %d", test.expectedInfo.compressedSize, ofi.compressedSize)
			}
		})
	}
}
