package tplinkipc

import (
	"encoding/binary"
	"os"

	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/rs/zerolog"
	opus2 "gopkg.in/hraban/opus.v2"
)

var log zerolog.Logger

func Init() {
	log = app.GetLogger("tplinkipc")
	streams.HandleFunc("tplinkipc", execHandle)
}

func execHandle(rawURL string) (prod core.Producer, err error) {
	// _, err = url.Parse(rawURL)
	// if err != nil {
	// 	return
	// }

	medias := []*core.Media{
		{
			Kind:      core.KindAudio,
			Direction: core.DirectionSendonly,
			Codecs:    []*core.Codec{core.ParseCodecString("opus")},
		},
	}

	return &Backchannel{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "tplinkipc",
			Protocol:   "pipe+tcp",
			Medias:     medias,
		},
	}, nil
}

type Backchannel struct {
	core.Connection
	waiting func()
}

func (c *Backchannel) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return nil, core.ErrCantGetTrack
}

func (c *Backchannel) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, track.Codec)
	dec, err := opus2.NewDecoder(int(track.Codec.ClockRate), int(track.Codec.Channels))
	if err != nil {
		return err
	}
	pcm := make([]int16, 1<<15)
	file, err := os.Create("/tmp/back.pcm")
	if err != nil {
		return err
	}
	sender.Handler = func(packet *rtp.Packet) {
		n, err := dec.Decode(packet.Payload, pcm)
		if err != nil {
			log.Err(err).Msg("Decoding packet failed")
		}
		size := n * int(track.Codec.Channels)
		buf := make([]byte, size*2)
		for i := range size {
			binary.LittleEndian.PutUint16(buf[i*2:], uint16(pcm[i]))
		}
		file.Write(buf)
	}
	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)
	c.waiting = func() {
		sender.Wait()
		file.Close()
	}
	return nil
}

func (c *Backchannel) Start() error {
	c.waiting()
	return nil
}

func (c *Backchannel) Stop() error {
	return c.Connection.Stop()
}
