package mongo_store

import (
	"context"

	"github.com/VineethReddy02/cortex-mongo-store/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type DBSchema struct {
	Hash  string `bson:"hash"`
	Range []byte `bson:"range"`
	Value []byte `bson:"value"`
}

func (c *server) WriteIndex(ctx context.Context, batch *grpc.WriteIndexRequest) (*empty.Empty, error) {
	for _, entry := range batch.Writes {
		c.Logger.Info("performing batch write. ", zap.String("Table name ", entry.TableName))
		fil := bson.D{{"hash", entry.HashValue}, {"range", entry.RangeValue}, {"value", entry.Value}}
		_, err := c.Client.Database(c.Cfg.Database).Collection(entry.TableName).InsertOne(ctx, fil)
		if err != nil {
			var errException mongo.WriteException
			errException = err.(mongo.WriteException)
			errCode := errException.WriteErrors[0].Code
			if errCode == 11000 {
				filter := bson.D{{"hash", entry.HashValue}, {"range", string(entry.RangeValue)}}
				_, err = c.Client.Database(c.Cfg.Database).Collection(entry.TableName).ReplaceOne(ctx, filter, fil)
				if err != nil {
					c.Logger.Error("failed to perform write index", zap.String("hash", entry.HashValue), zap.Error(err))
					return &empty.Empty{}, errors.WithStack(err)
				}
			} else {
				c.Logger.Error("failed to perform write index", zap.String("hash", entry.HashValue), zap.Error(err))
				return &empty.Empty{}, errors.WithStack(err)
			}
		}
	}
	return &empty.Empty{}, nil
}

func (c *server) QueryIndex(query *grpc.QueryIndexRequest, queryStreamer grpc.GrpcStore_QueryIndexServer) error {
	var q *mongo.Cursor
	var err error
	switch {
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual == nil:
		filter := bson.D{{"$and", []interface{}{bson.D{{"hash", query.HashValue}}, bson.D{{"range", bson.D{{"$gte", query.RangeValueStart}}}}, bson.D{{"range", bson.D{{"$lt", append(query.RangeValuePrefix, '\xff')}}}}}}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}

	case len(query.RangeValuePrefix) > 0 && query.ValueEqual != nil:
		filter := bson.D{{"$and", []interface{}{bson.D{{"hash", query.HashValue}}, bson.D{{"range", bson.D{{"$gte", query.RangeValueStart}}}}, bson.D{{"range", bson.D{{"$lt", append(query.RangeValuePrefix, '\xff')}}}}, bson.D{{"value", query.ValueEqual}}}}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}

	case len(query.RangeValueStart) > 0 && query.ValueEqual == nil:
		filter := bson.D{{"hash", query.HashValue}, {"range", bson.D{{"$gte", query.RangeValueStart}}}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}

	case len(query.RangeValueStart) > 0 && query.ValueEqual != nil:
		filter := bson.D{{"hash", query.HashValue}, {"range", bson.D{{"$gte", query.RangeValueStart}}}, {"value", query.ValueEqual}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}

	case query.ValueEqual == nil:
		filter := bson.D{{"hash", query.HashValue}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}

	case query.ValueEqual != nil:
		filter := bson.D{{"hash", query.HashValue}, {"value", query.ValueEqual}}
		q, err = c.Client.Database(c.Cfg.Database).Collection(query.TableName).Find(context.Background(), filter)
		if err != nil {
			c.Logger.Error("failed to perform query at index", zap.Error(err))
			return err
		}
	}

	b1 := &grpc.QueryIndexResponse{
		Rows: []*grpc.Row{},
	}
	defer q.Close(context.Background())
	for q.Next(context.Background()) {
		b := &DBSchema{}
		row := &grpc.Row{}
		err = q.Decode(&b)
		if err != nil {
			c.Logger.Error("failed to decode the result at index", zap.Error(err))
			return err
		}
		row.Value = b.Value
		row.RangeValue = b.Range
		// do something with result...
		b1.Rows = append(b1.Rows, row)
	}
	if err := q.Err(); err != nil {
		c.Logger.Error("failed at cursor level err at index", zap.Error(err))
		return err
	}

	// you can add custom logic here to break rows and send as stream instead of sending all at once.
	err = queryStreamer.Send(b1)
	if err != nil {
		c.Logger.Error("Unable to stream the results")
	}

	return nil
}

func (c *server) DeleteIndex(ctx context.Context, request *grpc.DeleteIndexRequest) (*empty.Empty, error) {
	for _, entry := range request.Deletes {
		filter := bson.D{{"hash", entry.HashValue}, {"range", entry.RangeValue}}
		_, err := c.Client.Database(c.Cfg.Database).Collection(entry.TableName).DeleteOne(ctx, filter, nil)
		if err != nil {
			c.Logger.Error("failed to perform batch write ", zap.Error(err))
			return &empty.Empty{}, errors.WithStack(err)
		}
	}
	return &empty.Empty{}, nil
}
