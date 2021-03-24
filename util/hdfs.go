package util

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"path"

	"github.com/colinmarc/hdfs/v2"
)

var (
	errMultipleNamenodeUrls = fmt.Errorf("Multiple namenode URLs specified")
)

type HDFSStream struct {
	Filename   string `json:"filename" json`
	Payload    []byte `json:"payload" json`
	Batch      int    `json:"batch_num"`
	TotalBatch int    `json:"total_batch"`
}

type HDFSConfig struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string
	// User specifies which HDFS user the client will act as. It is required
	// unless kerberos authentication is enabled, in which case it will be
	// determined from the provided credentials if empty.
	User string
	// UseDatanodeHostname specifies whether the client should connect to the
	// datanodes via hostname (which is useful in multi-homed setups) or IP
	// address, which may be required if DNS isn't available.
	UseDatanodeHostname bool
	// NamenodeDialFunc is used to connect to the datanodes. If nil, then
	// (&net.Dialer{}).DialContext is used.
	NamenodeDialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
	// DatanodeDialFunc is used to connect to the datanodes. If nil, then
	// (&net.Dialer{}).DialContext is used.
	DatanodeDialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	KerberosKeytabFile string
	KerberosConfigFile string
	KerberosRealm      string
	DownloadFolder string
}

func NewHDFSClient(cfg *HDFSConfig) (*hdfs.Client, error) {
	conf := hdfs.ClientOptions{
		Addresses:           cfg.Addresses,
		User:                cfg.User,
		UseDatanodeHostname: cfg.UseDatanodeHostname,
		NamenodeDialFunc:    cfg.NamenodeDialFunc,
		DatanodeDialFunc:    cfg.DatanodeDialFunc,
		KerberosServicePrincipleName: "hdfs/_HOST@" + cfg.KerberosRealm,
	}

	if cfg.KerberosKeytabFile != "" {
		if cfg.KerberosRealm == "" || cfg.KerberosConfigFile == "" {
			return nil, fmt.Errorf("KerberosRealm and KerberosConfigFile required")
		}

		krbClient, err := CreateKrbClient(cfg.User, cfg.KerberosRealm, cfg.KerberosConfigFile, cfg.KerberosKeytabFile)
		if err != nil {
			return nil, err
		}
		conf.KerberosClient = krbClient
	}

	return hdfs.NewClient(conf)
}

func NormalizeHDFSPaths(paths []string) ([]string, string, error) {
	namenode := ""
	cleanPaths := make([]string, 0, len(paths))

	for _, rawurl := range paths {
		url, err := url.Parse(rawurl)
		if err != nil {
			return nil, "", err
		}

		if url.Host != "" {
			if namenode != "" && namenode != url.Host {
				return nil, "", errMultipleNamenodeUrls
			}

			namenode = url.Host
		}

		cleanPaths = append(cleanPaths, path.Clean(url.Path))
	}

	return cleanPaths, namenode, nil
}
