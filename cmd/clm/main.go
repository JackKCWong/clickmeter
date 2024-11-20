package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "clm",
	Short: "A CLI for measuring the query performance of a clickhouse server",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	rootCmd.PersistentFlags().StringP("server", "s", "", "connection url. e.g. ch://user:pass@host:port/db or http://user:pass@host:port/db")
	rootCmd.PersistentFlags().Int16P("concurrent", "c", 1, "concurrent connections / queries")

	viper.BindPFlags(rootCmd.PersistentFlags())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
}

func connect(ctx context.Context, connUrl string) (driver.Conn, error) {
	remote, err := url.Parse(connUrl)
	if err != nil {
		return nil, err
	}

	username := remote.User.Username()
	password, isSet := remote.User.Password()
	if !isSet {
		password = ""
	}

	var proto clickhouse.Protocol
	if remote.Scheme == "http" {
		proto = clickhouse.HTTP
	} else {
		proto = clickhouse.Native
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Protocol(proto),
		Addr:     []string{fmt.Sprintf("%s:%s", remote.Hostname(), remote.Port())},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(remote.Path, "/"),
			Username: username,
			Password: password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "clickmeter", Version: "0.1"},
			},
		},

		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
	})

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	return conn, nil
}
