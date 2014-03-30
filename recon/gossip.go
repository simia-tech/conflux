/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (C) 2012  Casey Marshall <casey.marshall@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package recon

import (
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"log"
	"math/rand"
	"net"
	"time"
)

const GOSSIP = "gossip:"

// Gossip with remote servers, acting as a client.
func (p *Peer) Gossip() {
	timer := time.NewTimer(time.Duration(0))
	defer timer.Stop()
	for {
		select {
		case stop, _ := <-p.gossipStop:
			if stop != nil {
				stop <- new(interface{})
			}
			return
		case <-timer.C:
			var peer net.Addr
			var err error
			var conn net.Conn
			if !p.Enabled() {
				log.Println("Peer currently disabled.")
				goto DELAY
			}
			peer, err = p.choosePartner()
			if err != nil {
				if err != NoPartnersError {
					log.Println(GOSSIP, "choosePartner:", err)
				}
				goto DELAY
			}
			log.Println(GOSSIP, "Initiating recon with peer", peer)
			conn, err = net.DialTimeout(peer.Network(), peer.String(), time.Second)
			if err != nil {
				log.Println(GOSSIP, "Error connecting to", peer, ":", err)
				goto DELAY
			}
			err = p.InitiateRecon(conn)
			if err != nil {
				log.Println(GOSSIP, "Recon error:", err)
			}
		DELAY:
			// jitter the delay
			timer.Reset(time.Duration(p.GossipIntervalSecs()) * time.Second)
		}
	}
}

var NoPartnersError error = errors.New("No recon partners configured")
var IncompatiblePeerError error = errors.New("Remote peer configuration is not compatible")

func (p *Peer) choosePartner() (net.Addr, error) {
	partners, err := p.PartnerAddrs()
	if err != nil {
		return nil, err
	}
	if len(partners) == 0 {
		return nil, NoPartnersError
	}
	return partners[rand.Intn(len(partners))], nil
}

func (p *Peer) InitiateRecon(conn net.Conn) error {
	defer conn.Close()
	if p.ReadTimeout() > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(p.ReadTimeout())))
	}
	remoteConfig, err := p.handleConfig(conn, GOSSIP)
	if err != nil {
		return err
	}
	// Interact with peer
	return p.ExecCmd(func() error {
		return p.clientRecon(conn, remoteConfig)
	})
}

type msgProgress struct {
	elements *ZSet
	err      error
	flush    bool
	messages []ReconMsg
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("Reconciliation Done")

func (p *Peer) clientRecon(conn net.Conn, remoteConfig *Config) error {
	respSet := NewZSet()
	var pendingMessages []ReconMsg
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				log.Println(GOSSIP, "Reconcilation done.")
				break
			} else {
				WriteMsg(conn, &Error{&textMsg{Text: step.err.Error()}})
				log.Println(GOSSIP, step.err)
				break
			}
		} else {
			pendingMessages = append(pendingMessages, step.messages...)
			if step.flush {
				for _, msg := range pendingMessages {
					WriteMsg(conn, msg)
				}
				pendingMessages = nil
			}
		}
		log.Println(GOSSIP, "Add step:", step)
		respSet.AddAll(step.elements)
		log.Println(GOSSIP, "Recover set now:", respSet.Len(), "elements")
	}
	items := respSet.Items()
	if len(items) > 0 {
		log.Println(GOSSIP, "Sending recover:", len(items), "items")
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteConfig:   remoteConfig,
			RemoteElements: items}
	}
	return nil
}

func (p *Peer) interactWithServer(conn net.Conn) msgProgressChan {
	out := make(msgProgressChan)
	go func() {
		var resp *msgProgress
		for resp == nil || resp.err == nil {
			msg, err := ReadMsg(conn)
			if err != nil {
				log.Println(GOSSIP, "interact: msg err:", err)
				out <- &msgProgress{err: err}
				return
			}
			log.Println(GOSSIP, "interact: got msg:", msg)
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m)
			case *Elements:
				log.Println(GOSSIP, "Elements:", m.ZSet.Len())
				resp = &msgProgress{elements: m.ZSet}
			case *Done:
				resp = &msgProgress{err: ReconDone}
			case *Flush:
				resp = &msgProgress{elements: NewZSet(), flush: true}
			default:
				resp = &msgProgress{err: errors.New(fmt.Sprintf("Unexpected message: %v", m))}
			}
			out <- resp
		}
	}()
	return out
}

var ReconRqstPolyNotFound = errors.New("Peer should not receive a request for a non-existant node in ReconRqstPoly")

func (p *Peer) handleReconRqstPoly(rp *ReconRqstPoly) *msgProgress {
	remoteSize := rp.Size
	points := p.Points()
	remoteSamples := rp.Samples
	node, err := p.Node(rp.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := p.solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if err == LowMBar {
		log.Println(GOSSIP, "Low MBar")
		if node.IsLeaf() || node.Size() < (p.ThreshMult()*p.MBar()) {
			log.Println(GOSSIP, "Sending full elements for node:", node.Key())
			return &msgProgress{elements: NewZSet(), messages: []ReconMsg{&FullElements{ZSet: NewZSet(node.Elements()...)}}}
		}
	}
	if err != nil {
		log.Println(GOSSIP, "sending SyncFail because", err)
		return &msgProgress{elements: NewZSet(), messages: []ReconMsg{&SyncFail{}}}
	}
	log.Println(GOSSIP, "solved: localSet=", localSet, "remoteSet=", remoteSet)
	return &msgProgress{elements: remoteSet, messages: []ReconMsg{&Elements{ZSet: localSet}}}
}

func (p *Peer) solve(remoteSamples, localSamples []*Zp, remoteSize, localSize int, points []*Zp) (*ZSet, *ZSet, error) {
	var values []*Zp
	for i, x := range remoteSamples {
		values = append(values, Z(x.P).Div(x, localSamples[i]))
	}
	log.Println(GOSSIP, "Reconcile", values, points, remoteSize-localSize)
	return Reconcile(values, points, remoteSize-localSize)
}

func (p *Peer) handleReconRqstFull(rf *ReconRqstFull) *msgProgress {
	var localset *ZSet
	node, err := p.Node(rf.Prefix)
	if err == PNodeNotFound {
		localset = NewZSet()
	} else if err != nil {
		return &msgProgress{err: err}
	} else {
		localset = NewZSet(node.Elements()...)
	}
	localNeeds := ZSetDiff(rf.Elements, localset)
	remoteNeeds := ZSetDiff(localset, rf.Elements)
	log.Println(GOSSIP, "localNeeds=(", localNeeds.Len(), ") remoteNeeds=(", remoteNeeds.Len(), ")")
	return &msgProgress{elements: localNeeds, messages: []ReconMsg{&Elements{ZSet: remoteNeeds}}}
}
