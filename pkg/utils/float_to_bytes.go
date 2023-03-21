package utils

import (
	"encoding/binary"
	"fmt"
	"math"
)

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Float64ToBytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func main() {
	bytes := Float64ToBytes(8342.25311409682)
	fmt.Println(bytes)
	float := Float64frombytes(bytes)
	fmt.Println(float)
}
