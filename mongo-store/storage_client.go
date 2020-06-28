package mongo_store

import (
	"context"
	"flag"
	"strconv"
	"time"

	"github.com/VineethReddy02/cortex-mongo-store/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

// Config for a StorageClient
type Config struct {
	Addresses                string        `yaml:"addresses,omitempty"`
	GrpcServerPort           int           `json:"http_listen_port,yaml:"http_listen_port,omitempty"`
	Port                     int           `yaml:"port,omitempty"`
	Database                 string        `yaml:"database,omitempty"`
	Consistency              string        `yaml:"consistency,omitempty"`
	ReplicationFactor        int           `yaml:"replication_factor,omitempty"`
	DisableInitialHostLookup bool          `yaml:"disable_initial_host_lookup,omitempty"`
	SSL                      bool          `yaml:"SSL,omitempty"`
	HostVerification         bool          `yaml:"host_verification,omitempty"`
	CAPath                   string        `yaml:"CA_path,omitempty"`
	Auth                     bool          `yaml:"auth,omitempty"`
	Username                 string        `yaml:"username,omitempty"`
	Password                 string        `yaml:"password,omitempty"`
	Timeout                  time.Duration `yaml:"timeout,omitempty"`
	ConnectTimeout           time.Duration `yaml:"connect_timeout,omitempty"`
}

type server struct {
	Cfg    Config        `yaml:"cfg,omitempty"`
	Client *mongo.Client `yaml:"-"`
	Logger *zap.Logger
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, "mongo-store.addresses", "", "Host address of mongo store.")
	f.IntVar(&cfg.Port, "mongo-store.port", 27017, "Port that mongo is running on")
	f.StringVar(&cfg.Database, "mongo-store.database", "", "Database to use in Mongo.")
	f.BoolVar(&cfg.Auth, "mongo-store.auth", false, "Enable password authentication when connecting to mongo-store.")
	f.StringVar(&cfg.Username, "mongo-store.username", "", "Username to use when connecting to mongo-store.")
	f.StringVar(&cfg.Password, "mongo-store.password", "", "Password to use when connecting to mongo-store.")
	f.IntVar(&cfg.GrpcServerPort, "grpc.http_listen_port", 6688, "Port on which grpc mongo store should listen.")
}

func (c *server) session() error {
	var clientOptions *options.ClientOptions
	uri := "mongodb://" + c.Cfg.Addresses + ":" + strconv.Itoa(c.Cfg.Port)
	if c.Cfg.Password == "" && c.Cfg.Username == "" {
		clientOptions = options.Client().ApplyURI(uri)
	} else {
		credentials := options.Credential{
			Username: c.Cfg.Username,
			Password: c.Cfg.Password,
		}
		clientOptions = options.Client().ApplyURI(uri).SetAuth(credentials)
	}
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		c.Logger.Error("unable to create mongodb client", zap.Error(err))
		return err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		c.Logger.Error("unable to ping mongo database", zap.Error(err))
		return err
	}
	c.Client = client
	return nil
}

// create database will create the desired database if it doesn't exist.
func (c *server) createDatabase() *mongo.Database {
	d := c.Client.Database(c.Cfg.Database)
	return d
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg Config) (*server, error) {
	logger, _ := zap.NewProduction()
	c := &server{
		Cfg:    cfg,
		Logger: logger,
	}
	err := c.session()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.createDatabase()
	return c, nil
}

// Stop implement chunk.IndexClient.
func (c *server) Stop(context.Context, *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

// atomic writes.  Therefore we just do a bunch of writes in parallel.
type writeBatch struct {
	entries []chunk.IndexEntry
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

type readBatch struct {
	consumed   bool
	rangeValue []byte
	value      []byte
}

func (r *readBatch) Iterator() chunk.ReadBatchIterator {
	return &readBatchIter{
		readBatch: r,
	}
}

type readBatchIter struct {
	consumed bool
	*readBatch
}

func (b *readBatchIter) Next() bool {
	if b.consumed {
		return false
	}
	b.consumed = true
	return true
}

func (b *readBatchIter) RangeValue() []byte {
	return b.rangeValue
}

func (b *readBatchIter) Value() []byte {
	return b.value
}

// PutChunks implements chunk.ObjectClient.
func (c *server) PutChunks(ctx context.Context, chunks *grpc.PutChunksRequest) (*empty.Empty, error) {
	// Must provide a range key, even though its not used - hence "".
	for _, chunkInfo := range chunks.Chunks {
		c.Logger.Info("performing put chunks.", zap.String("table name", chunkInfo.TableName))
		fil := bson.D{{"hash", chunkInfo.Key}, {"range", []byte("")}, {"value", chunkInfo.Encoded}}
		_, err := c.Client.Database(c.Cfg.Database).Collection(chunkInfo.TableName).InsertOne(ctx, fil)
		if err != nil {
			var errException mongo.WriteException
			errException = err.(mongo.WriteException)
			errCode := errException.WriteErrors[0].Code
			// this checks the error cause due to duplicate insert.
			if errCode == 11000 {
				filter := bson.D{{"hash", chunkInfo.Key}}
				_, err = c.Client.Database(c.Cfg.Database).Collection(chunkInfo.TableName).ReplaceOne(ctx, filter, fil)
				if err != nil {
					c.Logger.Error("failed to perform put chunks for ", zap.String("hash", chunkInfo.Key), zap.Error(err))
					return &empty.Empty{}, errors.WithStack(err)
				}
			} else {
				c.Logger.Error("failed to perform put chunks for ", zap.String("hash", chunkInfo.Key), zap.Error(err))
				return &empty.Empty{}, errors.WithStack(err)
			}
		}
	}
	return &empty.Empty{}, nil
}

func (c *server) DeleteChunks(ctx context.Context, chunkID *grpc.ChunkID) (*empty.Empty, error) {
	return &empty.Empty{}, chunk.ErrNotSupported
}

func (c *server) GetChunks(input *grpc.GetChunksRequest, chunksStreamer grpc.GrpcStore_GetChunksServer) error {
	c.Logger.Info("performing get chunks.")
	var err error
	for _, chunkData := range input.Chunks {
		filter := bson.D{{"hash", chunkData.Key}}
		records, err := c.Client.Database(c.Cfg.Database).Collection(chunkData.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to do get chunks ", zap.Error(err))
			return err
		}
		for records.Next(context.Background()) {
			// To decode into a struct, use cursor.Decode()
			result := &DBSchema{}
			err := records.Decode(&result)
			if err != nil {
				c.Logger.Error("failed to decode get chunks ", zap.Error(err))
				return err
			}
			chunkData.Encoded = result.Value
			chunkData.Key = result.Hash
		}
	}
	// you can add custom logic here to break chunks to into smaller chunks and stream.
	// If size of chunks is large.
	res := &grpc.GetChunksResponse{Chunks: input.Chunks}
	err = chunksStreamer.Send(res)
	if err != nil {
		c.Logger.Error("Unable to stream the results")
	}
	return err
}
