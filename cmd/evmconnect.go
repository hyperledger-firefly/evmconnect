// Copyright © 2025 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/ethereum"
	fftmcmd "github.com/hyperledger-firefly/transaction-manager/cmd"
	"github.com/hyperledger-firefly/transaction-manager/pkg/fftm"
	txhandlerfactory "github.com/hyperledger-firefly/transaction-manager/pkg/txhandler/registry"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler/simple"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type EthereumConnector ethereum.Connector

var sigs = make(chan os.Signal, 1)

var rootCmd = &cobra.Command{
	Use:   "evmconnect",
	Short: "Hyperledger FireFly Connector for EVM based blockchains",
	Long:  ``,
	RunE: func(_ *cobra.Command, _ []string) error {
		return run()
	},
}

var cfgFile string

var connectorConfig config.Section

func init() {
	rootCmd.Flags().StringVarP(&cfgFile, "config", "f", "", "config file")
	rootCmd.AddCommand(versionCommand())
	rootCmd.AddCommand(configCommand())
	rootCmd.AddCommand(fftmcmd.ClientCommand())
	migrateCmd := fftmcmd.MigrateCommand(func() error {
		InitConfig()
		err := config.ReadConfig("evmconnect", cfgFile)
		config.SetupLogging(context.Background())
		return err
	})
	migrateCmd.PersistentFlags().StringVarP(&cfgFile, "config", "f", "", "config file")
	rootCmd.AddCommand(migrateCmd)
}

func Execute() error {
	return rootCmd.Execute()
}

func InitConfig() {
	fftm.InitConfig()
	connectorConfig = config.RootSection("connector")
	ethereum.InitConfig(connectorConfig)
	txhandlerfactory.RegisterHandler(&simple.TransactionHandlerFactory{})
}

func NewEthereumConnector(ctx context.Context, conf config.Section) (EthereumConnector, error) {
	return ethereum.NewEthereumConnector(ctx, conf)
}

func run() error {

	err := config.ReadConfig("evmconnect", cfgFile)

	// Setup logging after reading config (even if failed), to output header correctly
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	ctx = log.WithLogger(ctx, logrus.WithField("pid", fmt.Sprintf("%d", os.Getpid())))
	ctx = log.WithLogger(ctx, logrus.WithField("prefix", "evmconnect"))

	config.SetupLogging(ctx)

	// Deferred error return from reading config
	if err != nil {
		cancelCtx()
		return i18n.WrapError(ctx, err, i18n.MsgConfigFailed)
	}

	// Init connector
	c, err := NewEthereumConnector(ctx, connectorConfig)
	if err != nil {
		return err
	}
	m, err := fftm.NewManager(ctx, c)
	if err != nil {
		return err
	}

	// Setup signal handling to cancel the context, which shuts down the API Server
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.L(ctx).Infof("Shutting down due to %s", sig.String())
		cancelCtx()
	}()

	return runManager(ctx, m)
}

func runManager(ctx context.Context, m fftm.Manager) error {
	err := m.Start()
	if err != nil {
		return err
	}
	<-ctx.Done()
	m.Close()
	return nil
}
