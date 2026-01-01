package tplinkipc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	passwd, set := u.User.Password()
	if !set {
		return nil, fmt.Errorf("Password not set for user %s", u.User.Username())
	}

	talk := NewTplinkTalkConnection(
		bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		u.User.Username(), passwd, 0,
	)

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
		conn:    conn,
		talk:    talk,
		waiting: func() {},
	}, nil
}

type Backchannel struct {
	core.Connection
	conn    net.Conn
	talk    *TplinkTalkConnection
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
	err1 := c.Connection.Stop()
	err2 := c.talk.Stop()
	err3 := c.conn.Close()
	return errors.Join(err1, err2, err3)
}
