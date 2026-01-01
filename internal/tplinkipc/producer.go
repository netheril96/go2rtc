package tplinkipc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"time"

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

	ffmpegCmd := exec.Command(
		"ffmpeg",
		"-hide_banner", "-v", "error",
		"-fflags", "nobuffer", "-flags", "low_delay",
		"-f", "s16le", "-ar", "48000", "-ac", "2", "-i", "-",
		"-f", "mulaw", "-ar", "16000", "-ac", "1", "-",
	)
	ffmpegCmd.Stderr = os.Stderr
	ffmpegStdIn, err := ffmpegCmd.StdinPipe()
	if err != nil {
		return
	}
	ffmpegStdOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return
	}
	err = ffmpegCmd.Start()
	if err != nil {
		return
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

	success = true

	return &Backchannel{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "tplinkipc",
			Protocol:   "pipe+tcp",
			Medias:     medias,
		},
		conn:         conn,
		talk:         talk,
		ffmpegCmd:    ffmpegCmd,
		ffmpegStdIn:  ffmpegStdIn,
		ffmpegStdOut: ffmpegStdOut,
		waiting:      func() {},
	}, nil
}

type Backchannel struct {
	core.Connection
	conn         net.Conn
	talk         *TplinkTalkConnection
	ffmpegCmd    *exec.Cmd
	ffmpegStdIn  io.WriteCloser
	ffmpegStdOut io.ReadCloser
	waiting      func()
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
	err = c.talk.Start()
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second / 16)
		defer ticker.Stop()
		buf := make([]byte, 1000)
		for {
			<-ticker.C
			n, err := c.ffmpegStdOut.Read(buf)
			if err != nil {
				log.Err(err).Msg("Reading from ffmpeg failed")
				return
			}
			err = c.talk.SendPcm(buf[:n])
			if err != nil {
				log.Err(err).Msg("Sending to talk failed")
				return
			}
		}
	}()

	sender.Handler = func(packet *rtp.Packet) {
		n, err := dec.Decode(packet.Payload, pcm)
		if err != nil {
			log.Err(err).Msg("Decoding packet failed")
			return
		}
		size := n * int(track.Codec.Channels)
		buf := make([]byte, size*2)
		for i := range size {
			binary.LittleEndian.PutUint16(buf[i*2:], uint16(pcm[i]))
		}
		n, err = c.ffmpegStdIn.Write(buf)
		if err != nil {
			log.Err(err).Msg("Piping to ffmpeg failed")
			return
		}
	}
	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)
	c.waiting = func() {
		sender.Wait()
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
