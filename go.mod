module github.com/VineethReddy02/cortex-mongo-store

go 1.12

require (
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b // indirect
	github.com/cortexproject/cortex v0.6.1
	github.com/gocql/gocql v0.0.0-20200121121104-95d072f1b5bb // indirect
	github.com/golang/protobuf v1.4.0-rc.4.0.20200313231945-b860323f09d0
	github.com/opentracing/opentracing-go v1.1.1-0.20200124165624-2876d2018785 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.5.0 // indirect
	github.com/prometheus/prometheus v1.8.2-0.20200213233353-b90be6f32a33 // indirect
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/weaveworks/common v0.0.0-20200206153930-760e36ae819a // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200404213814-dbcf540c8800 // indirect
	go.mongodb.org/mongo-driver v1.1.0
	go.uber.org/zap v1.14.1
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.21.0
	sigs.k8s.io/yaml v1.1.0
)

replace github.com/Azure/azure-sdk-for-go => github.com/Azure/azure-sdk-for-go v36.2.0+incompatible

replace github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.0+incompatible

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Override reference that causes an error from Go proxy - see https://github.com/golang/go/issues/33558
replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
