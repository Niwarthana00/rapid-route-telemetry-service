package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"

	"github.com/yourorg/fleet-tracker-service/internal/config"
	"github.com/yourorg/fleet-tracker-service/internal/model"
)

type EventHandler func(ctx context.Context, ev *model.GPSEvent)

type Subscriber struct {
	client  pahomqtt.Client
	cfg     config.MQTTConfig
	handler EventHandler
	log     zerolog.Logger
}

func NewSubscriber(cfg config.MQTTConfig, handler EventHandler, log zerolog.Logger) (*Subscriber, error) {
	s := &Subscriber{cfg: cfg, handler: handler,
		log: log.With().Str("component", "mqtt").Logger()}

	opts := pahomqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetKeepAlive(cfg.KeepAlive).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(cfg.ReconnectWait).
		SetOnConnectHandler(s.onConnect).
		SetConnectionLostHandler(s.onConnectionLost)

	// TLS only applies to "tls://" or "ssl://" brokers. A plain "tcp://"
	// broker (e.g. local Mosquitto/EMQX in dev, no cert available) skips it.
	if strings.HasPrefix(cfg.Broker, "tls://") || strings.HasPrefix(cfg.Broker, "ssl://") {
		tlsCfg, err := buildTLS(cfg.TLSCACertPath)
		if err != nil {
			return nil, fmt.Errorf("mqtt: build TLS: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
	}

	s.client = pahomqtt.NewClient(opts)
	return s, nil
}

func (s *Subscriber) Connect(ctx context.Context) error {
	token := s.client.Connect()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("mqtt: connect: %w", err)
		}
	}
	s.log.Info().Str("broker", s.cfg.Broker).Msg("MQTT connected")
	return nil
}

func (s *Subscriber) Disconnect() {
	s.client.Disconnect(500)
	s.log.Info().Msg("MQTT disconnected")
}

func (s *Subscriber) onConnect(c pahomqtt.Client) {
	token := c.Subscribe(s.cfg.TopicPattern, s.cfg.QoS, s.messageHandler)
	if token.Wait() && token.Error() != nil {
		s.log.Error().Err(token.Error()).Msg("MQTT subscribe failed")
	}
	s.log.Info().Str("topic", s.cfg.TopicPattern).Msg("MQTT subscribed")
}

func (s *Subscriber) onConnectionLost(_ pahomqtt.Client, err error) {
	s.log.Error().Err(err).Msg("MQTT connection lost – reconnecting")
}

func (s *Subscriber) messageHandler(_ pahomqtt.Client, msg pahomqtt.Message) {
	// NOTE: the current topic (e.g. "fleet/gps") carries no bus/device id,
	// so every message is attributed to cfg.DefaultBusID for now.
	// TODO: switch back to per-bus identification once the topic is
	// "fleet/{busID}/gps" again, or the payload carries a bus/device id.
	busID := s.cfg.DefaultBusID

	var p model.GPSPayload
	if err := json.Unmarshal(msg.Payload(), &p); err != nil {
		s.log.Error().Err(err).Str("bus_id", busID).Msg("JSON decode failed")
		return
	}
	if !p.Fix {
		s.log.Debug().Str("bus_id", busID).Msg("no GPS fix – skipped")
		return
	}
	ev := &model.GPSEvent{
		BusID: busID, Lat: p.Lat, Lng: p.Lng,
		SpeedKmh: p.Spd, AltM: p.Alt,
		Satellites: p.Sat, HDOP: p.HDOP,
		Fix: p.Fix, SeatOccupied: p.SeatOccupied,
		DeviceTs: p.Ts, ReceivedAt: time.Now().UTC(),
	}
	s.handler(context.Background(), ev)
}

func buildTLS(caCertPath string) (*tls.Config, error) {
	pem, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parse CA cert failed")
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}