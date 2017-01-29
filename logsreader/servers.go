package logsreader

import "fmt"

// ConnectionInfo represents information about connection to server with nginx logs
type ConnectionInfo struct {
	Address  string
	Port     int
	UserName string
	Password string
}

// ServerName returns server name as Address:Port
func (conn ConnectionInfo) ServerName() string {
	return fmt.Sprintf("%s:%d", conn.Address, conn.Port)
}

func (conn ConnectionInfo) String() string {
	return conn.ServerName()
}
