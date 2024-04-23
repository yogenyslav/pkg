package postgres

type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Db           string `yaml:"db"`
	Ssl          bool   `yaml:"ssl"`
	RetryTimeout int    `yaml:"retry_timeout"`
}
