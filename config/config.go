package config

import "github.com/spf13/viper"

type Config struct {
	OPCDA OPCDA
	Kafka Kafka
}

type OPCDA struct {
	ProgId string   `yaml:"ProgId"`
	Nodes  []string `yaml:"Nodes"`
	Tags   []string `yaml:"Tags"`
}

type Kafka struct {
	Brokers   []string `yaml:"Brokers"`
	Topic     string   `yaml:"Topic"`
	Partition int      `yaml:"Partition"`
}

func ReadConfig(cfgName, cfgType, cfgPath string) *Config {
	var cfg Config

	viper.SetConfigName(cfgName)
	viper.SetConfigType(cfgType)
	viper.AddConfigPath(cfgPath)

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		panic(err)
	}

	return &cfg
}
