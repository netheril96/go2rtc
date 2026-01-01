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
	"syscall"
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

	_, set := u.User.Password()
	if !set {
		return nil, fmt.Errorf("Password not set for user %s", u.User.Username())
	}

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
		url: u,
	}, nil
}

type Backchannel struct {
	core.Connection
	url          *url.URL
	conn         net.Conn
	talk         *TplinkTalkConnection
	ffmpegCmd    *exec.Cmd
	ffmpegStdIn  io.WriteCloser
	ffmpegStdOut io.ReadCloser
}

func (c *Backchannel) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return nil, core.ErrCantGetTrack
}

func (c *Backchannel) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	if c.conn == nil {
		conn, err := net.Dial("tcp", c.url.Host)
		if err != nil {
			return err
		}
		c.conn = conn

		success := false
		defer func() {
			if !success {
				localErr := c.conn.Close()
				if localErr != nil {
					log.Err(localErr).Msg("Failed to close network connection")
				}
				c.conn = nil
			}
		}()

		passwd, _ := c.url.User.Password()

		c.ffmpegCmd = exec.Command(
			"ffmpeg",
			"-hide_banner", "-v", "error",
			"-fflags", "nobuffer", "-flags", "low_delay",
			"-f", "s16le", "-ar", fmt.Sprint(track.Codec.ClockRate), "-ac", fmt.Sprint(track.Codec.Channels), "-i", "-",
			"-f", "mulaw", "-ar", "16000", "-ac", "1", "-",
		)
		c.ffmpegCmd.Stderr = os.Stderr

		if c.ffmpegStdIn, err = c.ffmpegCmd.StdinPipe(); err != nil {
			return err
		}
		if c.ffmpegStdOut, err = c.ffmpegCmd.StdoutPipe(); err != nil {
			return err
		}
		if err = c.ffmpegCmd.Start(); err != nil {
			return err
		}

		c.talk = NewTplinkTalkConnection(
			bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
			c.url.User.Username(), passwd, 0,
		)
		success = true
	}

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
	return nil
}

func (c *Backchannel) Start() error {
	return c.ffmpegCmd.Wait()
}

func (c *Backchannel) Stop() error {
	errs := make([]error, 0, 4)
	if c.ffmpegCmd != nil {
		errs = append(errs, c.ffmpegCmd.Process.Signal(syscall.SIGTERM))
		c.ffmpegCmd = nil
	}
	if c.talk != nil {
		errs = append(errs, c.talk.Stop())
		c.talk = nil
	}
	if c.ffmpegStdIn != nil {
		errs = append(errs, c.ffmpegStdIn.Close())
		c.ffmpegStdIn = nil
	}
	if c.ffmpegStdOut != nil {
		errs = append(errs, c.ffmpegStdOut.Close())
		c.ffmpegStdOut = nil
	}
	if c.conn != nil {
		errs = append(errs, c.conn.Close())
		c.conn = nil
	}
	errs = append(errs, c.Connection.Stop())
	return errors.Join(errs...)
}
