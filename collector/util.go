package collector

import (
	"math"
	"math/big"

	"golang.org/x/exp/constraints"
)

func Pow[A, B constraints.Integer | constraints.Float](x A, n B) float64 {
	return math.Pow(float64(x), float64(n))
}

func Normalize(value *big.Int, multiplier uint64) float64 {
	bigF := new(big.Float).SetInt(value)
	f, _ := bigF.Quo(bigF, big.NewFloat(float64(multiplier))).Float64()
	return f
}
