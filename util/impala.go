package util

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"

	_ "github.com/likearthian/go-impala"
)

type ImpalaConfig struct {
	User          string
	Password      string
	AuthMech      string
	SSLTrustStore string
	TLS           string
	InsecureTLS   string
	Host          string
	Port          string
}

func CreateImpalaSqlDB(cfg ImpalaConfig) (*sql.DB, error) {
	query := url.Values{}
	query.Add("tls", cfg.TLS)
	query.Add("auth", cfg.AuthMech)
	query.Add("ca-cert", cfg.SSLTrustStore)
	query.Add("insecure-tls", cfg.InsecureTLS)

	u := &url.URL{
		Scheme:   "impala",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, cfg.Port),
		Path:     "default",
		RawQuery: query.Encode(),
	}

	fmt.Println("Impala DSN:\n", u.String())

	db, err := sql.Open("impala", u.String())
	if err != nil {
		return nil, err
	}

	return db, db.Ping()
}
