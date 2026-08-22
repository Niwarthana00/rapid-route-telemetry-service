package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	MQTT  MQTTConfig
	Kafka KafkaConfig
	HTTP  HTTPConfig
	Log   LogConfig
}

type MQTTConfig struct {
	Broker        string
	Username      string
	Password      string
	ClientID      string
	TopicPattern  string
	QoS           byte
	KeepAlive     time.Duration
	TLSCACertPath string
	ReconnectWait time.Duration
	// DefaultBusID is used while the topic carries no bus/device identifier
	// (e.g. a flat "fleet/gps" topic instead of "fleet/{busID}/gps").
	// TODO: remove once devices publish a real bus/device id (topic segment or payload field).
	DefaultBusID string
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
}

type HTTPConfig struct {
	ListenAddr string
}

type LogConfig struct {
	Level  string
	Pretty bool
}

func Load() (*Config, error) {
	return &Config{
		MQTT: MQTTConfig{
			Broker:        mustEnv("MQTT_BROKER"),
			Username:      mustEnv("MQTT_USERNAME"),
			Password:      mustEnv("MQTT_PASSWORD"),
			ClientID:      getEnv("MQTT_CLIENT_ID", "fleet-tracker-service"),
			TopicPattern:  getEnv("MQTT_TOPIC_PATTERN", "fleet/gps"),
			QoS:           byte(getEnvInt("MQTT_QOS", 0)),
			KeepAlive:     getEnvDuration("MQTT_KEEPALIVE", 30*time.Second),
			TLSCACertPath: getEnv("MQTT_TLS_CA_CERT", "/certs/root.crt"),
			ReconnectWait: getEnvDuration("MQTT_RECONNECT_WAIT", 5*time.Second),
			DefaultBusID:  getEnv("MQTT_DEFAULT_BUS_ID", "BUS-001"),
		},
		Kafka: KafkaConfig{
			Brokers: []string{getEnv("KAFKA_BROKER", "kafka:9092")},
			Topic:   getEnv("KAFKA_TOPIC", "iot-telemetry"),
		},
		HTTP: HTTPConfig{
			ListenAddr: getEnv("HTTP_LISTEN_ADDR", ":8080"),
		},
		Log: LogConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Pretty: getEnvBool("LOG_PRETTY", false),
		},
	}, nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var %q not set", key))
	}
	return v
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" { return v }
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil { return n }
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil { return b }
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil { return d }
	}
	return def
}