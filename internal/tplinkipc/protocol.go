package tplinkipc

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

type TplinkTalkConnection struct {
	rw         *bufio.ReadWriter
	user       string
	passwd     string
	packetizer rtp.Packetizer
}

func NewTplinkTalkConnection(rw *bufio.ReadWriter, user, passwd string, ssrc uint32) *TplinkTalkConnection {
	packetizer := rtp.NewPacketizer(1200, 102, ssrc, &codecs.G711Payloader{}, rtp.NewRandomSequencer(), 16000)
	return &TplinkTalkConnection{
		rw:         rw,
		user:       user,
		passwd:     passwd,
		packetizer: packetizer,
	}
}

type rtspResponse struct {
	StatusLine string
	Header     textproto.MIMEHeader
	Body       []byte
}

func (c *TplinkTalkConnection) writeAndRead(s string) (*rtspResponse, error) {
	_, err := fmt.Fprint(c.rw, s)
	if err != nil {
		return nil, err
	}
	err = c.rw.Flush()
	if err != nil {
		return nil, err
	}
	return c.readRtspResponse()
}

func (c *TplinkTalkConnection) Start() error {
	resp, err := c.writeAndRead("MULTITRANS rtsp://127.0.0.1/multitrans RTSP/1.0\r\nCSeq: 0\r\nContent-Length: 0\r\nX-Handshake: unused debug\r\nX-Client-Model: Android\r\nX-Client-UUID: 095250a6-c01d-4af3-8ca5-7536dd45a4ff19b6d3470c5\r\n\r\n")
	if err != nil {
		return err
	}
	var realm, nonce string
	for _, v := range resp.Header.Values("WWW-Authenticate") {
		if strings.HasPrefix(v, "Digest") {
			realm, nonce = parseDigestAuth(v)
			break
		}
	}
	if realm == "" || nonce == "" {
		return errors.New("No realm and nonce in the WWW-Authenticate header")
	}

	uri := "rtsp://127.0.0.1/multitrans"
	method := "MULTITRANS"
	authHeader := calculateDigestAuth(c.user, c.passwd, realm, nonce, method, uri)
	_, err = c.writeAndRead(fmt.Sprintf("MULTITRANS rtsp://127.0.0.1/multitrans RTSP/1.0\r\nCSeq: 1\r\nContent-Type: application/json\r\nX-Handshake: unused\r\nAuthorization: %s\r\nX-Client-Model: Android\r\nX-Client-UUID: 095250a6-c01d-4af3-8ca5-7536dd45a4ff19b6d3470c5\r\n\r\n", authHeader))
	if err != nil {
		return err
	}
	_, err = c.writeAndRead("MULTITRANS rtsp://127.0.0.1/multitrans RTSP/1.0\r\nCSeq: 2\r\nContent-Type: application/json\r\nContent-Length: 74\r\n\r\n{\"type\":\"request\",\"seq\":0,\"params\":{\"method\":\"get\",\"talk\":{\"mode\":\"aec\"}}}")
	if err != nil {
		return err
	}

	return nil
}

func (p *TplinkTalkConnection) Stop() error {
	_, err := p.writeAndRead("MULTITRANS rtsp://127.0.0.1/multitrans RTSP/1.0\r\nCSeq: 3\r\nX-Session-Id: 0\r\nContent-Type: application/json\r\nContent-Length: 65\r\n\r\n{\"type\":\"request\",\"seq\":1,\"params\":{\"method\":\"do\",\"stop\":\"null\"}}")
	if err != nil {
		return err
	}
	_, err = p.writeAndRead("TEARDOWN rtsp://127.0.0.1/multitrans RTSP/1.0\r\nCSeq: 4\r\n\r\n")
	return err
}

func (p *TplinkTalkConnection) SendPcm(pcm []byte) error {
	packets := p.packetizer.Packetize(pcm, uint32(len(pcm)))
	for _, packet := range packets {
		packet_bytes, err := packet.Marshal()
		if err != nil {
			return err
		}
		err = p.rw.WriteByte('$')
		if err != nil {
			return err
		}
		err = p.rw.WriteByte(1)
		if err != nil {
			return err
		}
		err = p.rw.WriteByte(byte(len(packet_bytes) >> 8))
		if err != nil {
			return err
		}
		err = p.rw.WriteByte(byte(len(packet_bytes) & 0xff))
		if err != nil {
			return err
		}
		_, err = p.rw.Write(packet_bytes)
		if err != nil {
			return err
		}
		err = p.rw.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func calculateDigestAuth(user, passwd, realm, nonce, method, uri string) string {
	ha1 := md5.Sum(fmt.Appendf(nil, "%s:%s:%s", user, realm, passwd))
	ha2 := md5.Sum(fmt.Appendf(nil, "%s:%s", method, uri))
	response := md5.Sum(fmt.Appendf(nil, "%x:%s:%x", ha1, nonce, ha2))

	return fmt.Sprintf("Digest username=\"%s\", realm=\"%s\", nonce=\"%s\", uri=\"%s\", response=\"%x\"",
		user, realm, nonce, uri, response)
}

func (c *TplinkTalkConnection) readRtspResponse() (*rtspResponse, error) {
	tp := textproto.NewReader(c.rw.Reader)
	line, err := tp.ReadLine()
	if err != nil {
		return nil, err
	}

	header, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}

	var body []byte
	if l := header.Get("Content-Length"); l != "" {
		length, err := strconv.Atoi(l)
		if err != nil {
			return nil, err
		}
		if length > 0 {
			body = make([]byte, length)
			if _, err := io.ReadFull(c.rw, body); err != nil {
				return nil, err
			}
		}
	}
	return &rtspResponse{
		StatusLine: line,
		Header:     header,
		Body:       body,
	}, nil
}

func parseDigestAuth(auth string) (realm string, nonce string) {
	const prefix = "Digest "
	if !strings.HasPrefix(auth, prefix) {
		return "", ""
	}
	for field := range strings.SplitSeq(auth[len(prefix):], ",") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		val := strings.Trim(parts[1], "\"")
		switch key {
		case "realm":
			realm = val
		case "nonce":
			nonce = val
		}
	}
	return
}
