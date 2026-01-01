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
	"sync"
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
		return nil, fmt.Errorf("parse url %s: %w", rawURL, err)
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
	url     *url.URL
	session *session
	mu      sync.Mutex
}

type session struct {
	conn         net.Conn
	talk         *TplinkTalkConnection
	ffmpegCmd    *exec.Cmd
	ffmpegStdIn  io.WriteCloser
	ffmpegStdOut io.ReadCloser
}

func newSession(url *url.URL, codec *core.Codec) (*session, error) {
	s := &session{}
	success := false
	defer func() {
		if success {
			return
		}
		localErr := s.Close()
		if localErr != nil {
			log.Err(localErr).Msg("Failed to close session")
		}
	}()
	var err error
	s.conn, err = net.Dial("tcp", url.Host)
	if err != nil {
		return nil, fmt.Errorf("dial tcp to host %s: %w", url.Host, err)
	}
	passwd, _ := url.User.Password()

	s.ffmpegCmd = exec.Command(
		"ffmpeg",
		"-hide_banner", "-v", "error",
		"-fflags", "nobuffer", "-flags", "low_delay",
		"-f", "s16le", "-ar", fmt.Sprint(codec.ClockRate), "-ac", fmt.Sprint(codec.Channels), "-i", "-",
		"-f", "mulaw", "-ar", "16000", "-ac", "1", "-",
	)
	s.ffmpegCmd.Stderr = os.Stderr

	if s.ffmpegStdIn, err = s.ffmpegCmd.StdinPipe(); err != nil {
		return nil, fmt.Errorf("ffmpeg stdin: %w", err)
	}
	if s.ffmpegStdOut, err = s.ffmpegCmd.StdoutPipe(); err != nil {
		return nil, fmt.Errorf("ffmpeg stdout: %w", err)
	}
	if err = s.ffmpegCmd.Start(); err != nil {
		return nil, fmt.Errorf("ffmpeg start: %w", err)
	}

	s.talk = NewTplinkTalkConnection(
		bufio.NewReadWriter(bufio.NewReader(s.conn), bufio.NewWriter(s.conn)),
		url.User.Username(), passwd, 0,
	)
	success = true
	return s, nil
}

func (s *session) Close() error {
	errs := make([]error, 0, 5)
	if s.talk != nil {
		if err := s.talk.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("talk stop: %w", err))
		}
		s.talk = nil
	}
	if s.ffmpegCmd != nil {
		if s.ffmpegCmd.Process != nil {
			ffmpegCmd := s.ffmpegCmd
			if err := ffmpegCmd.Process.Signal(syscall.SIGTERM); err != nil {
				errs = append(errs, fmt.Errorf("ffmpeg signal: %w", err))
			}
			go func() {
				ffmpegCmd.Wait()
			}()
		}
		s.ffmpegCmd = nil
	}
	if s.ffmpegStdOut != nil {
		if err := s.ffmpegStdOut.Close(); err != nil {
			errs = append(errs, fmt.Errorf("ffmpeg stdout close: %w", err))
		}
		s.ffmpegStdOut = nil
	}
	if s.ffmpegStdIn != nil {
		if err := s.ffmpegStdIn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("ffmpeg stdin close: %w", err))
		}
		s.ffmpegStdIn = nil
	}

	return errors.Join(errs...)
}

func (c *Backchannel) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return nil, core.ErrCantGetTrack
}

func (c *Backchannel) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.session != nil {
		log.Debug().Msg("Closing previous session")
		localErr := c.session.Close()
		if localErr != nil {
			log.Err(localErr).Msg("failed to close previous session")
		}
		c.session = nil
	}

	s, err := newSession(c.url, track.Codec)
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	success := false
	defer func() {
		if !success {
			s.Close()
			c.session = nil
		}
	}()
	c.session = s

	sender := core.NewSender(media, track.Codec)
	dec, err := opus2.NewDecoder(int(track.Codec.ClockRate), int(track.Codec.Channels))
	if err != nil {
		return fmt.Errorf("opus decoder: %w", err)
	}
	pcm := make([]int16, 1<<15)
	pcm_bytes := make([]byte, 2<<15)

	err = s.talk.Start()
	if err != nil {
		return fmt.Errorf("talk start: %w", err)
	}
	go func() {
		ticker := time.NewTicker(time.Second / 16)
		defer ticker.Stop()
		buf := make([]byte, 1000)
		for {
			<-ticker.C
			n, err := s.ffmpegStdOut.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Debug().Msg("Audio stream EOF, exiting normally...")
					return
				}
				log.Err(err).Msg("Reading from ffmpeg failed")
				return
			}
			err = s.talk.SendPcm(buf[:n])
			if err != nil {
				log.Err(err).Msg("Sending to talk failed")
				return
			}
		}
	}()

	timer := time.AfterFunc(10*time.Second, func() {
		c.mu.Lock()
		if c.session == s {
			log.Debug().Msg("Closing session due to to timeout")
			localErr := c.session.Close()
			if localErr != nil {
				log.Err(localErr).Msg("failed to close previous session")
			}
			c.session = nil
			sender.Close()
			for i, item := range c.Senders {
				if item == sender {
					c.Senders = append(c.Senders[:i], c.Senders[i+1:]...)
					break
				}
			}
		}
		c.mu.Unlock()
	})

	sender.Handler = func(packet *rtp.Packet) {
		timer.Reset(10 * time.Second)

		n, err := dec.Decode(packet.Payload, pcm)
		if err != nil {
			log.Err(err).Msg("Decoding packet failed")
			return
		}
		size := n * int(track.Codec.Channels)
		for i := range size {
			binary.LittleEndian.PutUint16(pcm_bytes[i*2:], uint16(pcm[i]))
		}
		n, err = s.ffmpegStdIn.Write(pcm_bytes[:size])
		if err != nil {
			log.Err(err).Msg("Piping to ffmpeg failed")
			return
		}
	}
	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)
	success = true
	return nil
}

func (c *Backchannel) Start() error {
	c.mu.Lock()
	s := c.session
	c.mu.Unlock()

	if s == nil {
		return nil
	}
	if err := s.ffmpegCmd.Wait(); err != nil {
		return fmt.Errorf("ffmpeg wait: %w", err)
	}
	return nil
}

func (c *Backchannel) Stop() error {
	c.mu.Lock()
	s := c.session
	c.session = nil
	c.mu.Unlock()

	var errs []error
	if s != nil {
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("session close: %w", err))
		}
	}
	c.sessions = nil
	if err := c.Connection.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("connection stop: %w", err))
	}
	return errors.Join(errs...)
}
