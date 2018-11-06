package recon

import (
	"bufio"
	"log"
	"net"

	"gopkg.in/errgo.v1"

	"github.com/pkg/errors"
	"github.com/simia-tech/conflux"
)

func (p *Peer) Reconcilate(network, address string, limit int) ([][]byte, bool, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()

	remoteConfig, err := p.handleConfig(conn, GOSSIP, "")
	if err != nil {
		return nil, false, errors.Wrap(err, "handle config failed")
	}

	localNeeds, done, err := p.reconcilate(conn, remoteConfig, limit)
	if err != nil {
		return nil, false, err
	}

	result := [][]byte{}
	for _, element := range localNeeds.Items() {
		zb := element.Bytes()
		for len(zb) < SksZpNbytes {
			zb = append(zb, byte(0))
		}
		zb = zb[:len(zb)-1]
		result = append(result, zb)
	}
	return result, done, nil
}

func (p *Peer) reconcilate(conn Conn, remoteConfig *Config, limit int) (*conflux.ZSet, bool, error) {
	w := bufio.NewWriter(conn)
	localNeeds := conflux.NewZSet()

	count := 0
	pendingMessages := []ReconMsg{}
	for count < limit {
		p.setReadDeadline("CLIENT", conn, defaultTimeout)
		message, err := ReadMsg(conn)
		if err != nil {
			return nil, false, errors.Wrap(err, "read messagw failed")
		}
		switch m := message.(type) {
		case *ReconRqstPoly:
			elements, message, err := p.handleReconRequestPoly(m)
			if err != nil {
				return nil, false, err
			}
			localNeeds.AddAll(elements)
			count += elements.Len()
			pendingMessages = append(pendingMessages, message)
			if length := elements.Len(); length > 0 {
				log.Printf("got %d elements (%d total) by poly", length, count)
			}
		case *ReconRqstFull:
			elements, message, err := p.handleReconRequestFull(m)
			if err != nil {
				return nil, false, err
			}
			localNeeds.AddAll(elements)
			count += elements.Len()
			pendingMessages = append(pendingMessages, message)
			if length := elements.Len(); length > 0 {
				log.Printf("got %d elements (%d total) by full", length, count)
			}
		// case *Elements:
		// 	log.Printf("got %d elements", m.ZSet.Len())
		// 	resp = &msgProgress{elements: m.ZSet}
		case *Done:
			return localNeeds, true, nil
		case *Flush:
			for _, message := range pendingMessages {
				if err := WriteMsg(w, message); err != nil {
					return nil, false, err
				}
			}
			if err := w.Flush(); err != nil {
				return nil, false, err
			}
			pendingMessages = []ReconMsg{}
		default:
			return nil, false, errors.Errorf("got unexpected message %T", m)
		}
	}
	return localNeeds, false, nil
}

func (p *Peer) handleReconRequestPoly(rp *ReconRqstPoly) (*conflux.ZSet, ReconMsg, error) {
	remoteSize := rp.Size
	points := p.ptree.Points()
	remoteSamples := rp.Samples
	node, err := p.ptree.Node(rp.Prefix)
	if err == ErrNodeNotFound {
		return nil, nil, ErrReconRqstPolyNotFound
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := p.solvex(remoteSamples, localSamples, remoteSize, localSize, points)
	if errgo.Cause(err) == conflux.ErrLowMBar {
		// log.Printf("low MBar")
		if node.IsLeaf() || node.Size() < (p.settings.ThreshMult*p.settings.MBar) {
			log.Printf("sending full elements / node %v", node.Key())
			elements, err := node.Elements()
			if err != nil {
				return nil, nil, err
			}
			return conflux.NewZSet(), &FullElements{ZSet: conflux.NewZSet(elements...)}, nil
			// return &msgProgress{elements: conflux.NewZSet(), messages: []ReconMsg{
			// 	&FullElements{ZSet: conflux.NewZSet(elements...)}}}
		} else {
			err = errors.Wrapf(err, "bs=%v leaf=%v size=%d", node.Key(), node.IsLeaf(), node.Size())
		}
	}
	if err != nil {
		// log.Printf("sending SyncFail")
		return conflux.NewZSet(), &SyncFail{}, nil
	}
	// log.Printf("solved / local %v / remote %v", localSet, remoteSet)
	return remoteSet, &Elements{ZSet: localSet}, nil
}

func (p *Peer) handleReconRequestFull(rf *ReconRqstFull) (*conflux.ZSet, ReconMsg, error) {
	var localset *conflux.ZSet
	node, err := p.ptree.Node(rf.Prefix)
	if err == ErrNodeNotFound {
		localset = conflux.NewZSet()
	} else if err != nil {
		return nil, nil, err
	} else {
		elements, err := node.Elements()
		if err != nil {
			return nil, nil, err
		}
		localset = conflux.NewZSet(elements...)
	}
	localNeeds := conflux.ZSetDiff(rf.Elements, localset)
	remoteNeeds := conflux.ZSetDiff(localset, rf.Elements)
	// log.Printf("request full / local needs %d / remote needs %d", localNeeds.Len(), remoteNeeds.Len())
	return localNeeds, &Elements{ZSet: remoteNeeds}, nil
}

func (p *Peer) solvex(remoteSamples, localSamples []*conflux.Zp, remoteSize, localSize int, points []*conflux.Zp) (*conflux.ZSet, *conflux.ZSet, error) {
	var values []*conflux.Zp
	for i, x := range remoteSamples {
		values = append(values, conflux.Z(x.P).Div(x, localSamples[i]))
	}
	return conflux.Reconcile(values, points, remoteSize-localSize)
}
