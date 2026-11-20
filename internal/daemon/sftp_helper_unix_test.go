//go:build unix

package daemon

import (
	"net"

	"github.com/pkg/sftp"
)

// sftpClient is the subset used by the forwarding test.
type sftpClient interface {
	Create(path string) (*sftp.File, error)
	Close() error
}

type testSFTPClient struct{ c *sftp.Client }

func (t testSFTPClient) Create(path string) (*sftp.File, error) { return t.c.Create(path) }
func (t testSFTPClient) Close() error                           { return t.c.Close() }

// newTestSFTPClient builds a pkg/sftp client over one pinned data stream.
func newTestSFTPClient(conn net.Conn) (sftpClient, error) {
	c, err := sftp.NewClientPipe(conn, conn)
	if err != nil {
		return nil, err
	}
	return testSFTPClient{c: c}, nil
}
