package block

import (
	"fmt"
)

type BlockID struct {
	FilePath    string
	BlockNumber int64
}

func (b BlockID) String() string {
	return fmt.Sprintf("%s:%d", b.FilePath, b.BlockNumber)
}
