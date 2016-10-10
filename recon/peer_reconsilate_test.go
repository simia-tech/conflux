package recon_test

import (
	"log"
	"testing"

	"github.com/simia-tech/conflux/recon"
	"github.com/stretchr/testify/require"
)

func TestPeerReconcilate(t *testing.T) {
	settings := recon.DefaultSettings()
	settings.Filters = []string{"yminsky.dedup", "yminsky.merge"}
	settings.ReconNet = recon.NetworkTCP
	settings.ReconAddr = "localhost:4600"

	prefixTree := &recon.MemPrefixTree{}
	prefixTree.Init()
	require.NoError(t, prefixTree.Create())

	peer := recon.NewPeer(settings, prefixTree)

	md5s, err := peer.Reconcilate()
	require.NoError(t, err)
	require.True(t, len(md5s) > 10)

	for index, md5 := range md5s {
		log.Printf("md5 %2d: %x", index, md5)
	}
}
