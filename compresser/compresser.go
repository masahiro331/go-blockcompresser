package compresser

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"
	"os"
	"unsafe"

	"golang.org/x/xerrors"
)

var (
	_ CompressedFile = &File{}

	BitMaskLUT = []uint8{
		0b00000001,
		0b00000010,
		0b00000100,
		0b00001000,
		0b00010000,
		0b00100000,
		0b01000000,
		0b10000000,
	}
)

const (
	MagicByte = 0x4742434d
	ChunkSize = 8
)

type CompressedFile interface {
	io.ReadWriteSeeker
	io.Closer
}

func Create(name string, blockSize uint32, fileSize uint64) (CompressedFile, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, xerrors.Errorf("failed to create: %w", err)
	}
	return &File{
		File: *f,
		Header: Header{
			Core: Core{
				Magic:     MagicByte,
				BlockSize: blockSize,
				FSSize:    fileSize,
			},
		},
	}, nil
}

func NewCompressedFile(f *os.File) (*File, error) {
	var header Header
	_, err := f.Seek(-int64(unsafe.Sizeof(header.Core)), 2)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek: %w", err)
	}

	if err = binary.Read(f, binary.BigEndian, &header.Core); err != nil {
		return nil, xerrors.Errorf("failed to parse header core: %w", err)
	}
	_, err = f.Seek(int64(header.Core.Size), 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek: %w", err)
	}

	for i := uint64(0); i < header.Core.MapSize; i++ {
		node := Node{}
		if err = binary.Read(f, binary.BigEndian, &node.PrePopCount); err != nil {
			return nil, xerrors.Errorf("failed to parse pre popcount: %w", err)
		}
		if err = binary.Read(f, binary.BigEndian, &node.CompressedMap); err != nil {
			return nil, xerrors.Errorf("failed to parse compressed map: %w", err)
		}
		header.CompressedTable = append(header.CompressedTable, node)
	}
	f.Seek(0, 0)
	return &File{
		File:   *f,
		Header: header,
	}, nil
}

type Core struct {
	Magic     uint32 // Magic byte
	BlockSize uint32 // Block Size
	FSSize    uint64 // DeCompressedFile Size
	MapSize   uint64 // CompressedMap Size
	Size      uint64 // CompressedFile Size ignore Header
}

type Header struct {
	Core            Core
	CompressedTable []Node // All zero block is 1, other 0
}

type Node struct {
	PrePopCount   uint64
	CompressedMap uint8
}

type File struct {
	Header Header

	chunkedBuffer bytes.Buffer

	currentByteOffset  uint64
	currentBlockOffset uint64
	os.File
}

func (f *File) getWroteBytesLength() uint64 {
	return uint64(len(f.Header.CompressedTable))*
		uint64(ChunkSize)*
		uint64(f.Header.Core.BlockSize) +
		uint64(f.chunkedBuffer.Len())
}

func (f *File) Write(buf []byte) (int, error) {
	if len(buf) != int(f.Header.Core.BlockSize) {
		return 0, xerrors.Errorf("invalid bytes size error write only %d byte length", f.Header.Core.BlockSize)
	}
	i, err := f.chunkedBuffer.Write(buf)
	if err != nil {
		return 0, xerrors.Errorf("failed to write chunked buffer: %w", err)
	}
	if f.chunkedBuffer.Len()/int(f.Header.Core.BlockSize) == ChunkSize {
		_, err := f.flush()
		if err != nil {
			return 0, xerrors.Errorf("failed to flush: %w", err)
		}
	}

	return i, nil
}

func (f *File) flush() (int, error) {
	if f.chunkedBuffer.Len()/int(f.Header.Core.BlockSize) != ChunkSize {
		return 0, xerrors.Errorf("invalid chunk size(%d) error ", f.chunkedBuffer.Len())
	}

	var cn int
	compressedMap := uint8(0x00) // 0000 0000

	for i := 0; i < ChunkSize; i++ {
		buf := make([]byte, f.Header.Core.BlockSize)
		n, err := f.chunkedBuffer.Read(buf)
		if err != nil {
			return 0, xerrors.Errorf("failed to read chunk buffer: %w", err)
		}
		if n != int(f.Header.Core.BlockSize) {
			return 0, xerrors.Errorf("invalid read chunk buffer size(%d) error", n)
		}

		if IsAllBytesZero(buf) {
			compressedMap = compressedMap | 1<<i
			continue
		}

		_, err = f.File.Write(buf)
		if err != nil {
			return 0, xerrors.Errorf("failed to write file error: %w", err)
		}
		cn += n
	}
	f.Header.CompressedTable = append(f.Header.CompressedTable, Node{CompressedMap: compressedMap})
	f.Header.Core.Size += uint64(cn)
	return cn, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	panic("")
}

func (f *File) Read(buf []byte) (int, error) {
	if len(buf) != int(f.Header.Core.BlockSize) {
		return 0, xerrors.Errorf("invalid bytes size error read only %d byte length", f.Header.Core.BlockSize)
	}

	if uint64(len(f.Header.CompressedTable)) <= f.currentBlockOffset/ChunkSize {
		return 0, io.EOF
	}

	defer func() {
		f.currentBlockOffset++
	}()

	node := f.Header.CompressedTable[f.currentBlockOffset/ChunkSize]
	if node.CompressedMap&BitMaskLUT[f.currentBlockOffset%ChunkSize] != 0 {
		return int(f.Header.Core.BlockSize), nil
	}

	i, err := f.File.Read(buf)
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, xerrors.Errorf("failed to read file: %w", err)
	}

	return i, nil
}

func (f *File) Close() error {
	tailBuf := make([]byte, int(f.Header.Core.BlockSize)*ChunkSize-f.chunkedBuffer.Len())
	_, err := f.chunkedBuffer.Write(tailBuf)
	if err != nil {
		return xerrors.Errorf("failed to write tail buffer: %w", err)
	}
	_, err = f.flush()
	if err != nil {
		return xerrors.Errorf("failed to flush with tail buffer: %w", err)
	}

	prePopcount := uint64(0)
	for _, node := range f.Header.CompressedTable {
		node.PrePopCount = prePopcount
		if err := binary.Write(&f.File, binary.BigEndian, node); err != nil {
			return err
		}

		popCount := bits.OnesCount8(node.CompressedMap)
		prePopcount += uint64(popCount)
	}
	f.Header.Core.MapSize = uint64(len(f.Header.CompressedTable))
	if err := binary.Write(&f.File, binary.BigEndian, f.Header.Core); err != nil {
		return err
	}
	return f.File.Close()
}

func IsAllBytesZero(data []byte) bool {
	for _, b := range data {
		if b != 0x00 {
			return false
		}
	}
	return true
}
