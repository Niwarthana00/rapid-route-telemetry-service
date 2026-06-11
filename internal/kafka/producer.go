package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/rs/zerolog"

	"github.com/yourorg/fleet-tracker-service/internal/config"
	"github.com/yourorg/fleet-tracker-service/internal/model"
)

type Producer struct {
	writer *kafkago.Writer
	log    zerolog.Logger
}

func NewProducer(cfg config.KafkaConfig, log zerolog.Logger) *Producer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafkago.LeastBytes{},
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		AllowAutoTopicCreation: true,
	}
	return &Producer{
		writer: w,
		log:    log.With().Str("component", "kafka-producer").Logger(),
	}
}

func (p *Producer) Produce(ctx context.Context, ev *model.GPSEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka: marshal event: %w", err)
	}
	msg := kafkago.Message{
		Key:   []byte(ev.BusID),
		Value: data,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.log.Error().Err(err).Str("bus_id", ev.BusID).Msg("Kafka produce failed")
		return fmt.Errorf("kafka: write message: %w", err)
	}
	p.log.Debug().Str("bus_id", ev.BusID).Msg("Kafka produce OK")
	return nil
}

func (p *Producer) Handle(ctx context.Context, ev *model.GPSEvent) {
	if err := p.Produce(ctx, ev); err != nil {
		p.log.Warn().Err(err).Str("bus_id", ev.BusID).Msg("event dropped")
	}
}

func (p *Producer) Close() error {
	p.log.Info().Msg("Kafka writer closed")
	return p.writer.Close()
}