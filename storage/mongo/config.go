package mongo

type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Db           string `yaml:"db"`
	AuthType     string `yaml:"auth_type"`
	RetryTimeout int    `yaml:"retry_timeout"`
}
