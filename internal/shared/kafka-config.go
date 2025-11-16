package shared

type KafkaConfig struct {
	Topic         string
	ConsumerGroup string
	Host          string
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Topic:         "local_topic",
		ConsumerGroup: "local_cg",
		Host:          "localhost",
	}
}
