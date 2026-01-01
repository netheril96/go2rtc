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
	"sync/atomic"
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
	url *url.URL

	wg         sync.WaitGroup
	quitSignal atomic.Pointer[chan struct{}]
}

func loop(url *url.URL, codec *core.Codec, decodedPcm <-chan []byte, quitSignal chan struct{}) error {
	conn, err := net.Dial("tcp", url.Host)
	if err != nil {
		return fmt.Errorf("dial tcp to host %s: %w", url.Host, err)
	}
	defer conn.Close()
	passwd, _ := url.User.Password()

	ffmpegCmd := exec.Command(
		"ffmpeg",
		"-hide_banner", "-v", "error",
		"-fflags", "nobuffer", "-flags", "low_delay",
		"-f", "s16le", "-ar", fmt.Sprint(codec.ClockRate), "-ac", fmt.Sprint(codec.Channels), "-i", "-",
		"-f", "mulaw", "-ar", "16000", "-ac", "1", "-",
	)
	ffmpegCmd.Stderr = os.Stderr

	ffmpegStdOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdout: %w", err)
	}
	defer ffmpegStdOut.Close()

	ffmpegStdIn, err := ffmpegCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdin: %w", err)
	}
	defer ffmpegStdIn.Close()

	if err = ffmpegCmd.Start(); err != nil {
		return fmt.Errorf("ffmpeg start: %w", err)
	}
	defer func() {
		ffmpegCmd.Process.Signal(syscall.SIGTERM)
		exitErr := ffmpegCmd.Wait()
		if exitErr != nil {
			log.Warn().Err(exitErr).Msg("ffmpeg exited abnormally")
		} else {
			log.Debug().Msg("ffmpeg exited normally")
		}
		quitSignal <- struct{}{}
	}()

	talk := NewTplinkTalkConnection(
		bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		url.User.Username(), passwd, 0,
	)

	err = talk.Start()
	if err != nil {
		return fmt.Errorf("talk start: %w", err)
	}
	defer talk.Stop()

	go func() {
		defer func() { quitSignal <- struct{}{} }()
		ticker := time.NewTicker(time.Second / 16)
		defer ticker.Stop()
		buf := make([]byte, 1000)
		for {
			<-ticker.C
			n, err := ffmpegStdOut.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Debug().Msg("Audio stream EOF, exiting normally...")
					return
				}
				log.Err(err).Msg("Reading from ffmpeg failed")
				return
			}
			err = talk.SendPcm(buf[:n])
			if err != nil {
				log.Err(err).Msg("Sending to talk failed")
				return
			}
		}
	}()

	for {
		select {
		case <-quitSignal:
			return nil
		case pcm, ok := <-decodedPcm:
			if !ok {
				return nil
			}
			n, err := ffmpegStdIn.Write(pcm)
			if err != nil {
				return fmt.Errorf("piping to ffmpeg failed: %w", err)
			}
			if n < len(pcm) {
				return fmt.Errorf("piping to ffmpeg is incomplete (%d/%d)", n, len(pcm))
			}
		}
	}
}

func (c *Backchannel) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return nil, core.ErrCantGetTrack
}

func (c *Backchannel) reinitQuitSignal() chan struct{} {
	newSignal := make(chan struct{}, 15)
	oldSignal := c.quitSignal.Swap(&newSignal)
	if oldSignal != nil && *oldSignal != nil {
		log.Debug().Msg("Closing previous session")
		*oldSignal <- struct{}{}
		close(*oldSignal)
	}
	return newSignal
}

func (c *Backchannel) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	decodedChannel := make(chan []byte, 5)
	quitSignal := c.reinitQuitSignal()

	c.wg.Go(func() {
		err := loop(c.url, track.Codec, decodedChannel, quitSignal)
		if err != nil {
			log.Err(err).Msg("Loop failed")
		}
	})

	sender := core.NewSender(media, track.Codec)
	dec, err := opus2.NewDecoder(int(track.Codec.ClockRate), int(track.Codec.Channels))
	if err != nil {
		return fmt.Errorf("opus decoder: %w", err)
	}
	pcm := make([]int16, 1<<15)

	sender.Handler = func(packet *rtp.Packet) {
		n, err := dec.Decode(packet.Payload, pcm)
		if err != nil {
			log.Err(err).Msg("Decoding packet failed")
			return
		}
		size := n * int(track.Codec.Channels)
		pcm_bytes := make([]byte, size*2)
		for i := range size {
			binary.LittleEndian.PutUint16(pcm_bytes[i*2:], uint16(pcm[i]))
		}
		decodedChannel <- pcm_bytes
	}
	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)
	return nil
}

func (c *Backchannel) Start() error {
	c.wg.Wait()
	return nil
}

func (c *Backchannel) Stop() error {
	oldSignal := c.quitSignal.Swap(nil)
	if oldSignal != nil && *oldSignal != nil {
		*oldSignal <- struct{}{}
		close(*oldSignal)
	}
	return nil
}
