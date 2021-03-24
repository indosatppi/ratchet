package util

import (
	"database/sql"
	"fmt"

	"github.com/godror/godror"
)

type OracleConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Xid      string
	LibDir   string
}

func CreateOracleSqlDB(cfg OracleConfig) (*sql.DB, error) {
	var p = godror.ConnectionParams{}
	p.Username = cfg.User
	p.Password = godror.NewPassword(cfg.Password)
	p.ConnectString = fmt.Sprintf("%s:%s/%s", cfg.Host, cfg.Port, cfg.Xid)
	p.LibDir = cfg.LibDir

	db := sql.OpenDB(godror.NewConnector(p))
	return db, db.Ping()
}
