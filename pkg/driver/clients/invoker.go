package clients

import (
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/vhive-serverless/loader/pkg/common"
	"github.com/vhive-serverless/loader/pkg/config"
	"github.com/vhive-serverless/loader/pkg/metric"
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

type Invoker interface {
	Invoke(*common.Function, *common.RuntimeSpecification) (bool, *metric.ExecutionRecord)
}

func CreateInvoker(cfg *config.Configuration, announceDoneExe *sync.WaitGroup, readOpenWhiskMetadata *sync.Mutex) Invoker {
	switch strings.ToLower(cfg.LoaderConfiguration.Platform) {
	case common.PlatformAWSLambda:
		return newAWSLambdaInvoker(announceDoneExe)
	case common.PlatformDirigent:
		if cfg.DirigentConfiguration == nil {
			logrus.Fatal("Failed to create invoker: dirigent configuration is required for platform 'dirigent'")
		}
		if strings.ToLower(cfg.DirigentConfiguration.Backend) == common.BackendDandelion || cfg.LoaderConfiguration.InvokeProtocol != "grpc" {
			return newHTTPInvoker(cfg)
		} else {
			return newGRPCInvoker(cfg.LoaderConfiguration, ExecutorRPC{})
		}
	case common.PlatformKnative:
		if cfg.LoaderConfiguration.InvokeProtocol == "grpc" {
			if !cfg.LoaderConfiguration.VSwarm {
				return newGRPCInvoker(cfg.LoaderConfiguration, ExecutorRPC{})
			} else {
				return newGRPCInvoker(cfg.LoaderConfiguration, SayHelloRPC{})
			}
		} else {
			return newHTTPInvoker(cfg)
		}
	case common.PlatformOpenWhisk:
		return newOpenWhiskInvoker(announceDoneExe, readOpenWhiskMetadata)
	default:
		logrus.Fatal("Unsupported platform.")
	}

	return nil
}
