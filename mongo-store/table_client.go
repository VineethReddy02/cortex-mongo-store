package mongo_store

import (
	"context"

	rpc "github.com/VineethReddy02/cortex-mongo-store/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.uber.org/zap"
)

func (c *server) ListTables(context.Context, *empty.Empty) (*rpc.ListTablesResponse, error) {
	c.Logger.Info("listing the tables ")
	result := &rpc.ListTablesResponse{}
	tables, err := c.Client.Database(c.Cfg.Database).ListCollectionNames(context.Background(), bson.D{}, nil)
	if err != nil {
		c.Logger.Error("failed in listing the tables", zap.Error(err))
		return result, err
	}
	for _, name := range tables {
		result.TableNames = append(result.TableNames, name)
	}
	return result, nil
}

func (c *server) CreateTable(ctx context.Context, req *rpc.CreateTableRequest) (*empty.Empty, error) {
	c.Logger.Info("creating the table ", zap.String("Table Name", req.Desc.Name))
	index := mongo.IndexModel{
		Keys:    bsonx.Doc{{"hash", bsonx.Int32(1)}, {"range", bsonx.Int32(1)}, {"value", bsonx.Int32(-1)}},
		Options: options.Index().SetUnique(true),
	}
	_, err := c.Client.Database(c.Cfg.Database).Collection(req.Desc.Name).Indexes().CreateOne(ctx, index)
	if err != nil {
		c.Logger.Error("failed to create table", zap.Error(err))
	}
	return &empty.Empty{}, err
}

func (c *server) DeleteTable(ctx context.Context, tableName *rpc.DeleteTableRequest) (*empty.Empty, error) {
	c.Logger.Info("deleting the table ", zap.String("Table Name", tableName.TableName))
	err := c.Client.Database(c.Cfg.Database).Collection(tableName.TableName).Drop(ctx)
	if err != nil {
		c.Logger.Error("failed to delete the table %s", zap.Error(err))
	}
	return &empty.Empty{}, errors.WithStack(err)
}

func (c *server) DescribeTable(ctx context.Context, tableName *rpc.DescribeTableRequest) (*rpc.DescribeTableResponse, error) {
	c.Logger.Info("describing the table ", zap.String("Table Name", tableName.TableName))
	name := tableName.TableName
	return &rpc.DescribeTableResponse{
		Desc: &rpc.TableDesc{
			Name: name,
		},
		IsActive: true,
	}, nil
}

func (c *server) UpdateTable(context.Context, *rpc.UpdateTableRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
