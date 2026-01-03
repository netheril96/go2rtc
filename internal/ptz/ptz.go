package ptz

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/rs/zerolog"
	"github.com/use-go/onvif"
	"github.com/use-go/onvif/media"
	"github.com/use-go/onvif/ptz"
	sdk_media "github.com/use-go/onvif/sdk/media"
	sdk_ptz "github.com/use-go/onvif/sdk/ptz"
	xsd "github.com/use-go/onvif/xsd/onvif"
)

var log zerolog.Logger
var ptzs map[string]PTZConfig

type PTZConfig struct {
	Name     string `yaml:"name"`
	HostPort string `yaml:"hostport"`
	User     string `yaml:"user"`
	Pass     string `yaml:"pass"`
}

func Init() {
	var cfg struct {
		Ptzs map[string]PTZConfig `yaml:"ptzs"`
	}

	app.LoadConfig(&cfg)
	ptzs = cfg.Ptzs

	log = app.GetLogger("ptz")

	api.HandleFunc("/api/ptz", handleFunc)
}

type ptzRequest struct {
	Name string  `json:"name"`
	Pan  float64 `json:"p"`
	Tilt float64 `json:"t"`
	Zoom float64 `json:"z"`
}

func handleFunc(w http.ResponseWriter, r *http.Request) {
	var req ptzRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conf, ok := ptzs[req.Name]
	if !ok {
		http.Error(w, "ptz not found", http.StatusNotFound)
		return
	}

	params := onvif.DeviceParams{
		Xaddr:    conf.HostPort,
		Username: conf.User,
		Password: conf.Pass,
	}
	if !strings.HasPrefix(params.Xaddr, "http") {
		params.Xaddr = "http://" + params.Xaddr
	}

	dev, err := onvif.NewDevice(params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	getProfilesResponse, err := sdk_media.Call_GetProfiles(r.Context(), dev, media.GetProfiles{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(getProfilesResponse.Profiles) == 0 {
		http.Error(w, "no profiles found", http.StatusInternalServerError)
		return
	}
	token := getProfilesResponse.Profiles[0].Token
	relMove := ptz.RelativeMove{
		ProfileToken: token,
		Translation: xsd.PTZVector{
			PanTilt: xsd.Vector2D{
				X: req.Pan,
				Y: req.Tilt,
			},
			Zoom: xsd.Vector1D{
				X: req.Zoom,
			},
		},
		Speed: xsd.PTZSpeed{},
	}
	if _, err = sdk_ptz.Call_RelativeMove(r.Context(), dev, relMove); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
