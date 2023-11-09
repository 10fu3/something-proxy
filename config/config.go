package config

import (
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

import "log"

type Value struct {
	EtcdHost string `env:"ETCD_HOST" envDefault:"localhost"`
	EtcdPort string `env:"ETCD_PORT" envDefault:"2379"`
}

func Get() *Value {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
	var conf Value
	if err := env.Parse(&conf); err != nil {
		return &Value{}
	}
	return &conf
}
