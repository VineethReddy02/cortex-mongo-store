# Cortex MONGO store

This is gRPC based mongo store for Cortex to store both indexes & chunks.

Below are the steps to run mongo store with cortex

Run Mongo database:

```yaml
docker run -d -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=admin -p 27017:27017 mongo
```

Save below configuration to ```grpc-mongo.yaml``` file.

```yaml
cfg:
  http_listen_port: 6688 #This is port gRPC server exposes
  addresses: localhost
  database: cortex
  username: admin
  password: admin
  port: 27017
```

Steps to run gRPC mongo store:

Run Cortex gRPC server for Mongo:

```yaml
cd bin
./cortex-mongo-store --config.file=grpc-mongo-store.yaml
```

Now run Cortex and configure the gRPC store details in Cortex ```--config.file```  under ```schema``` & ```storage``` as mentioned below

```yaml
# Use gRPC based storage backend -for both index store and chunks store.
schema:
  configs:
  - from: 2019-07-29
    store: grpc-store
    object_store: grpc-store
    schema: v10
    index:
      prefix: index_
      period: 168h
    chunks:
      prefix: chunk_
      period: 168h

storage:
  grpc-store: 
    address: localhost:6688
```

Cheers!