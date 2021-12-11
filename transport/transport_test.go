package transport_test

import (
	"context"
	"time"

	"github.com/muirglacier/aw/channel"
	"github.com/muirglacier/aw/dht"
	"github.com/muirglacier/aw/handshake"
	"github.com/muirglacier/aw/transport"
	"github.com/muirglacier/aw/wire"
	"github.com/muirglacier/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transport", func() {
	Describe("Dial", func() {
		Context("when failing to connect to peer", func() {
			It("should create an expiry and delete peer after expiration", func() {
				privKey := id.NewPrivKey()
				self := privKey.Signatory()
				h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(privKey))
				client := channel.NewClient(
					channel.DefaultOptions(),
					self)
				table := dht.NewInMemTable(self)
				transport := transport.New(
					transport.DefaultOptions().
						WithClientTimeout(10*time.Second).
						WithOncePoolOptions(handshake.DefaultOncePoolOptions().WithMinimumExpiryAge(10*time.Second)).
						WithExpiry(5*time.Second).
						WithPort(uint16(3333)),
					self,
					client,
					h,
					table,
				)

				privKey2 := id.NewPrivKey()
				table.AddPeer(privKey2.Signatory(),
					wire.NewUnsignedAddress(wire.TCP, ":3334", uint64(time.Now().UnixNano())))
				_, ok := table.PeerAddress(privKey2.Signatory())
				Expect(ok).To(BeTrue())

				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					transport.Send(ctx, privKey2.Signatory(), wire.Msg{})
				}()

				time.Sleep(7 * time.Second)
				cancel()

				_, ok = table.PeerAddress(privKey2.Signatory())
				Expect(ok).To(BeFalse())
			})
		})
	})
})
