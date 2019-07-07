package cast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
)

type Caster interface {
	Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error
	AcceptCast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error
}

type caster struct {
	dht      dht.DHT
	messages protocol.MessageSender
	events   protocol.EventSender
	me       protocol.PeerID
}

func NewCaster(dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender, me protocol.PeerID) Caster {
	return &caster{
		dht:      dht,
		messages: messages,
		events:   events,
		me:       me,
	}
}

func (caster *caster) Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error {
	peerAddr, err := caster.dht.PeerAddress(to)
	if err != nil {
		return newErrCastingMessage(to, err)
	}
	if peerAddr == nil {
		return newErrCastingMessage(to, fmt.Errorf("nil peer address"))
	}

	send := protocol.MessageSend{
		To:      peerAddr.NetworkAddress(),
		Message: protocol.NewMessage(protocol.V1, protocol.Cast, body),
	}

	select {
	case <-ctx.Done():
		return newErrCastingMessage(to, ctx.Err())
	case caster.messages <- send:
		return nil
	}
}

func (caster *caster) AcceptCast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error {
	if to.Equal(caster.me) {
		event := protocol.EventMessageReceived{
			Time:    time.Now(),
			Message: body,
		}
		select {
		case <-ctx.Done():
			return newErrCastingMessage(to, ctx.Err())
		case caster.events <- event:
			return nil
		}
	}
	return newErrCastingMessage(to, fmt.Errorf("no peer available for forwarding"))
}

type ErrCastingMessage struct {
	error
	PeerID protocol.PeerID
}

func newErrCastingMessage(peerID protocol.PeerID, err error) error {
	return ErrCastingMessage{
		error:  fmt.Errorf("error casting to peer=%v: %v", peerID, err),
		PeerID: peerID,
	}
}
