/*
 * MIT License
 *
 * Copyright (c) 2023 EASL and the vHive community
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package clients

import (
	"context"
	"strings"
	"time"
	"sync"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/vhive-serverless/loader/pkg/common"
	"github.com/vhive-serverless/loader/pkg/config"
	"github.com/vhive-serverless/loader/pkg/workload/proto"
	helloworld "github.com/vhive-serverless/vSwarm/utils/protobuf/helloworld"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mc "github.com/vhive-serverless/loader/pkg/metric"
)

// ConnectionPool manages a pool of gRPC connections
type ConnectionPool struct {
    mu          sync.Mutex
    connections map[string][]*grpc.ClientConn
    maxIdle     int
    dialOptions []grpc.DialOption
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxIdleConns int, dialOptions []grpc.DialOption) *ConnectionPool {
    return &ConnectionPool{
        connections: make(map[string][]*grpc.ClientConn),
        maxIdle:     maxIdleConns,
        dialOptions: dialOptions,
    }
}

// Get retrieves a connection from the pool or creates a new one
func (p *ConnectionPool) Get(endpoint string) (*grpc.ClientConn, error) {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if conns, ok := p.connections[endpoint]; ok && len(conns) > 0 {
        // Get last connection from the pool
        lastIdx := len(conns) - 1
        conn := conns[lastIdx]
        p.connections[endpoint] = conns[:lastIdx]
        return conn, nil
    }
    
    // No connection available in pool, create a new one
    conn, err := grpc.NewClient("passthrough:///"+endpoint, p.dialOptions...)
    if err != nil {
        return nil, err
    }
    
    return conn, nil
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(endpoint string, conn *grpc.ClientConn) {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if conn == nil {
        return
    }
    
    // Check if we're at max idle connections
    if conns, exists := p.connections[endpoint]; exists && len(conns) >= p.maxIdle {
        // Close excess connection instead of adding to pool
        if err := conn.Close(); err != nil {
            logrus.Warnf("Error closing excess gRPC connection: %v", err)
        }
        return
    }
    
    // Add connection to pool
    p.connections[endpoint] = append(p.connections[endpoint], conn)
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    for endpoint, conns := range p.connections {
        for _, conn := range conns {
            if err := conn.Close(); err != nil {
                logrus.Warnf("Error closing gRPC connection to %s: %v", endpoint, err)
            }
        }
        delete(p.connections, endpoint)
    }
}

type invoker interface {
	Invoke(function *common.Function, runtimeSpec *common.RuntimeSpecification, conn *grpc.ClientConn, record *mc.ExecutionRecord, executionCxt context.Context) bool
}

type ExecutorRPC struct {
}

func (i ExecutorRPC) Invoke(function *common.Function, runtimeSpec *common.RuntimeSpecification, conn *grpc.ClientConn, record *mc.ExecutionRecord, executionCxt context.Context) bool {
	grpcClient := proto.NewExecutorClient(conn)

	response, err := grpcClient.Execute(executionCxt, &proto.FaasRequest{
		Message:           "nothing",
		RuntimeInMilliSec: uint32(runtimeSpec.Runtime),
		MemoryInMebiBytes: uint32(runtimeSpec.Memory),
	})

	if err != nil {
		logrus.Debugf("gRPC timeout exceeded for function %s - %s", function.Name, err)

		record.ConnectionTimeout = true // WithBlock deprecated in new gRPC interface
		record.FunctionTimeout = true

		return false
	}

	record.Instance = extractInstanceName(response.GetMessage())
	record.ActualDuration = response.DurationInMicroSec

	if strings.HasPrefix(response.GetMessage(), "FAILURE - mem_alloc") {
		record.MemoryAllocationTimeout = true
	} else {
		record.ActualMemoryUsage = common.Kib2Mib(response.MemoryUsageInKb)
	}

	logrus.Tracef("(Replied)\t %s: %s, %.2f[ms], %d[MiB]", function.Name, response.Message,
		float64(response.DurationInMicroSec)/1e3, common.Kib2Mib(response.MemoryUsageInKb))

	return true
}

type SayHelloRPC struct {
}

func (i SayHelloRPC) Invoke(function *common.Function, runtimeSpec *common.RuntimeSpecification, conn *grpc.ClientConn, record *mc.ExecutionRecord, executionCxt context.Context) bool {
	grpcClient := helloworld.NewGreeterClient(conn)
	response, err := grpcClient.SayHello(executionCxt, &helloworld.HelloRequest{
		Name: "Invoke Relay",
		VHiveMetadata: MakeVHiveMetadata(
			uuid.New().String(),
			uuid.New().String(),
			time.Now().UTC(),
		),
	})
	if err != nil {
		logrus.Debugf("gRPC timeout exceeded for function %s - %s", function.Name, err)
		record.ConnectionTimeout = true
		record.FunctionTimeout = true

		return false
	}
	record.ActualDuration = 0
	record.Instance = extractSwarmFunction(response.GetMessage())
	record.ActualMemoryUsage = common.Kib2Mib(0) //Memory usage may not be available for all vSwarm benchmarks

	return true
}

type grpcInvoker struct {
	cfg     *config.LoaderConfiguration
	invoker invoker
	connPool     *ConnectionPool
}

func newGRPCInvoker(cfg *config.LoaderConfiguration, invoker invoker) *grpcInvoker {
    // Create dial options
    var dialOptions []grpc.DialOption
    dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if strings.Contains(cfg.Platform, common.PlatformDirigent) {
        dialOptions = append(dialOptions, grpc.WithAuthority("pooled-authority"))
    }
    
    if cfg.EnableZipkinTracing {
        dialOptions = append(dialOptions, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
    }
    
    // Create a connection pool with a reasonable number of idle connections per endpoint
    // Adjust the maxIdleConns value based on your workload characteristics
    return &grpcInvoker{
        cfg:      cfg,
        invoker:  invoker,
        connPool: NewConnectionPool(100, dialOptions),
    }
}

func (i *grpcInvoker) Invoke(function *common.Function, runtimeSpec *common.RuntimeSpecification) (bool, *mc.ExecutionRecord) {
	logrus.Tracef("(Invoke)\t %s: %d[ms], %d[MiB]", function.Name, runtimeSpec.Runtime, runtimeSpec.Memory)

	record := &mc.ExecutionRecord{
		ExecutionRecordBase: mc.ExecutionRecordBase{
			RequestedDuration: uint32(runtimeSpec.Runtime * 1e3),
		},
	}

	////////////////////////////////////
	// INVOKE FUNCTION
	////////////////////////////////////
	start := time.Now()
	record.StartTime = start.UnixMicro()

	// var dialOptions []grpc.DialOption
	// dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if strings.Contains(i.cfg.Platform, common.PlatformDirigent) {
	// 	dialOptions = append(dialOptions, grpc.WithAuthority(function.Name)) // Dirigent specific
	// }
	// if i.cfg.EnableZipkinTracing {
	// 	dialOptions = append(dialOptions, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	// }

	grpcStart := time.Now()

    conn, err := i.connPool.Get(function.Endpoint)
	if err != nil {
		logrus.Debugf("Failed to get a gRPC connection from pool - %v\n", err)

		record.ResponseTime = time.Since(start).Microseconds()
		record.ConnectionTimeout = true

		return false, record
	}
	// defer gRPCConnectionClose(conn)

	record.GRPCConnectionEstablishTime = time.Since(grpcStart).Microseconds()
	executionCxt, cancelExecution := context.WithTimeout(context.Background(), time.Duration(i.cfg.GRPCFunctionTimeoutSeconds)*time.Second)
	// defer cancelExecution()
	success := i.invoker.Invoke(function, runtimeSpec, conn, record, executionCxt)
	cancelExecution()
	// Return connection to pool instead of closing
	i.connPool.Put(function.Endpoint, conn)

	record.ResponseTime = time.Since(start).Microseconds()
	logrus.Tracef("(E2E Latency) %s: %.2f[ms]\n", function.Name, float64(record.ResponseTime)/1e3)
	return success, record
}

func (i *grpcInvoker) Close() {
    if i.connPool != nil {
        i.connPool.Close()
    }
}

func extractInstanceName(data string) string {
	indexOfHyphen := strings.LastIndex(data, common.FunctionNamePrefix)
	if indexOfHyphen == -1 {
		return data
	}

	return data[indexOfHyphen:]
}
func extractSwarmFunction(data string) string {
	index := strings.Index(data, "fn: ")
	verticalBarIndex := strings.Index(data, " |")
	if index == -1 {
		return data
	}
	if verticalBarIndex == -1 {
		return data[index+4:]
	}
	return data[index+4 : verticalBarIndex]
}

func gRPCConnectionClose(conn *grpc.ClientConn) {
	if conn == nil {
		return
	}

	if err := conn.Close(); err != nil {
		logrus.Warnf("Error while closing gRPC connection - %s\n", err)
	}
}
