package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/VineethReddy02/cortex-mongo-store/grpc"
	"github.com/VineethReddy02/cortex-mongo-store/mongo-store"
	"go.mongodb.org/mongo-driver/mongo"
	"sigs.k8s.io/yaml"

	"go.uber.org/zap"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/pkg/errors"
	g "google.golang.org/grpc"
)

type server struct {
	Cfg    mongo_store.Config `yaml:"cfg,omitempty"`
	Client *mongo.Client      `yaml:"-"`
	Logger *zap.Logger
}

func (c *server) RegisterFlags(f *flag.FlagSet) {
	c.Cfg.RegisterFlags(f)
}

const (
	configFileOption = "config.file"
	configExpandENV  = "config.expand-env"
)

func main() {
	var cfg server
	s := g.NewServer()
	flagext.RegisterFlags(&cfg)
	configFile, expandENV := parseConfigFileParameter(os.Args[1:])
	if configFile != "" {
		if err := LoadConfig(configFile, expandENV, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error loading config from %s: %v\n", configFile, err)
			os.Exit(1)
		}
	}

	s1, err := mongo_store.NewStorageClient(cfg.Cfg)
	if err != nil {
		log.Fatalf("Failed to created new storage client")
	}
	grpc.RegisterGrpcStoreServer(s, s1)

	gRPCServerAddress := "localhost:" + strconv.Itoa(cfg.Cfg.GrpcServerPort)
	lis, err := net.Listen("tcp", gRPCServerAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Parse -config.file and -config.expand-env option via separate flag set, to avoid polluting default one and calling flag.Parse on it twice.
func parseConfigFileParameter(args []string) (configFile string, expandEnv bool) {
	// ignore errors and any output here. Any flag errors will be reported by main flag.Parse() call.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	// usage not used in these functions.
	fs.StringVar(&configFile, configFileOption, "", "")
	fs.BoolVar(&expandEnv, configExpandENV, false, "")

	// Try to find -config.file and -config.expand-env option in the flags. As Parsing stops on the first error, eg. unknown flag, we simply
	// try remaining parameters until we find config flag, or there are no params left.
	// (ContinueOnError just means that flag.Parse doesn't call panic or os.Exit, but it returns error, which we ignore)
	for len(args) > 0 {
		_ = fs.Parse(args)
		args = args[1:]
	}

	return
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, expandENV bool, cfg *server) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	if expandENV {
		buf = expandEnv(buf)
	}

	err = yaml.UnmarshalStrict(buf, cfg)
	if err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

// expandEnv replaces ${var} or $var in config according to the values of the current environment variables.
// The replacement is case-sensitive. References to undefined variables are replaced by the empty string.
// A default value can be given by using the form ${var:default value}.
func expandEnv(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		keyAndDefault := strings.SplitN(key, ":", 2)
		key = keyAndDefault[0]

		v := os.Getenv(key)
		if v == "" && len(keyAndDefault) == 2 {
			v = keyAndDefault[1] // Set value to the default.
		}
		return v
	}))
}
