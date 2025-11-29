package shared

type KafkaPartitionStrategies string

const (
	CooperativeStickyStrategy KafkaPartitionStrategies = "cooperative-sticky"
	RoundRobin                KafkaPartitionStrategies = "roundrobin"
)

type KafkaConfig struct {
	DefaultTopic             string
	Host                     string
	ConsumerGroup            string
	ParititionAssignStrategy KafkaPartitionStrategies
	NumPartitions            int
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		ParititionAssignStrategy: CooperativeStickyStrategy,
		DefaultTopic:             "local_topic_sticky1",
		Host:                     "localhost",
		ConsumerGroup:            "local_cg1",
		NumPartitions:            4,
	}
}
