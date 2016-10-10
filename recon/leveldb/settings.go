package leveldb

import (
	"github.com/simia-tech/conflux/recon"
)

type Config struct {
	Path string `toml:"path"`
}

type Settings struct {
	recon.Settings

	LevelDB Config `toml:"leveldb"`
}
