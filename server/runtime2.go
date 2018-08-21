// Copyright 2018 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"database/sql"
	"github.com/golang/protobuf/jsonpb"
	"github.com/heroiclabs/nakama/rtapi"
	"github.com/heroiclabs/nakama/social"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrRuntimeRPCNotFound = errors.New("RPC function not found")
)

const API_PREFIX = "/nakama.api.Nakama/"
const RTAPI_PREFIX = "*rtapi.Envelope_"

type (
	Runtime2RpcFunction               func(queryParams map[string][]string, userID, username string, expiry int64, sessionID, clientIP, clientPort, payload string) (string, error, codes.Code)
	Runtime2BeforeRtFunction          func(logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) (*rtapi.Envelope, error)
	Runtime2AfterRtFunction           func(logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) error
	Runtime2MatchmakerMatchedFunction func(entries []*MatchmakerEntry) (string, error)
)

type RuntimeExecutionMode int

const (
	RuntimeExecutionModeRunOnce RuntimeExecutionMode = iota
	RuntimeExecutionModeRPC
	RuntimeExecutionModeBefore
	RuntimeExecutionModeAfter
	RuntimeExecutionModeMatch
	RuntimeExecutionModeMatchmaker
)

func (e RuntimeExecutionMode) String() string {
	switch e {
	case RuntimeExecutionModeRunOnce:
		return "run_once"
	case RuntimeExecutionModeRPC:
		return "rpc"
	case RuntimeExecutionModeBefore:
		return "before"
	case RuntimeExecutionModeAfter:
		return "after"
	case RuntimeExecutionModeMatch:
		return "match"
	case RuntimeExecutionModeMatchmaker:
		return "matchmaker"
	}

	return ""
}

type RuntimeProvider interface {
	// TODO
}

type Runtime2 struct {
	providerGo  RuntimeProvider
	providerLua RuntimeProvider

	rpcFunctions              map[string]Runtime2RpcFunction
	beforeRtFunctions         map[string]Runtime2BeforeRtFunction
	afterRtFunctions          map[string]Runtime2AfterRtFunction
	matchmakerMatchedFunction Runtime2MatchmakerMatchedFunction
}

func NewRuntime2(logger, startupLogger *zap.Logger, db *sql.DB, jsonpbMarshaler *jsonpb.Marshaler, jsonpbUnmarshaler *jsonpb.Unmarshaler, config Config, socialClient *social.Client, leaderboardCache LeaderboardCache, sessionRegistry *SessionRegistry, matchRegistry MatchRegistry, tracker Tracker, router MessageRouter) (*Runtime2, error) {
	runtimeConfig := config.GetRuntime()
	startupLogger.Info("Initialising runtime", zap.String("path", runtimeConfig.Path))

	if err := os.MkdirAll(runtimeConfig.Path, os.ModePerm); err != nil {
		return nil, err
	}

	paths := make([]string, 0)
	if err := filepath.Walk(runtimeConfig.Path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			startupLogger.Error("Error listing runtime path", zap.String("path", path), zap.Error(err))
			return err
		}

		// Ignore directories.
		if !f.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		startupLogger.Error("Failed to list runtime path", zap.Error(err))
		return nil, err
	}

	goModules, goRpcFunctions, goBeforeRtFunctions, goAfterRtFunctions, goMatchmakerMatchedFunction, goProvider, err := NewRuntimeProviderGo(logger, startupLogger, db, config, socialClient, leaderboardCache, sessionRegistry, matchRegistry, tracker, router, runtimeConfig.Path, paths)
	if err != nil {
		startupLogger.Error("Error initialising Go runtime provider", zap.Error(err))
		return nil, err
	}

	luaModules, luaRpcFunctions, luaBeforeRtFunctions, luaAfterRtFunctions, luaMatchmakerMatchedFunction, luaProvider, err := NewRuntimeProviderLua(logger, startupLogger, db, jsonpbMarshaler, jsonpbUnmarshaler, config, socialClient, leaderboardCache, sessionRegistry, matchRegistry, tracker, router, runtimeConfig.Path, paths)
	if err != nil {
		startupLogger.Error("Error initialising Lua runtime provider", zap.Error(err))
		return nil, err
	}

	allModules := make([]string, 0, len(goModules)+len(luaModules))
	for _, module := range luaModules {
		allModules = append(allModules, module)
	}
	for _, module := range goModules {
		allModules = append(allModules, module)
	}
	startupLogger.Info("Found runtime modules", zap.Int("count", len(allModules)), zap.Strings("modules", allModules))

	allRpcFunctions := make(map[string]Runtime2RpcFunction, len(goRpcFunctions)+len(luaRpcFunctions))
	for id, fn := range luaRpcFunctions {
		allRpcFunctions[id] = fn
		startupLogger.Info("Registered Lua runtime RPC function invocation", zap.String("id", id))
	}
	for id, fn := range goRpcFunctions {
		allRpcFunctions[id] = fn
		startupLogger.Info("Registered Go runtime RPC function invocation", zap.String("id", id))
	}

	allBeforeRtFunctions := make(map[string]Runtime2BeforeRtFunction, len(goBeforeRtFunctions)+len(luaBeforeRtFunctions))
	for id, fn := range luaBeforeRtFunctions {
		allBeforeRtFunctions[id] = fn
		startupLogger.Info("Registered Lua runtime Before function invocation", zap.String("id", strings.TrimLeft(strings.TrimLeft(id, API_PREFIX), RTAPI_PREFIX)))
	}
	for id, fn := range goBeforeRtFunctions {
		allBeforeRtFunctions[id] = fn
		startupLogger.Info("Registered Go runtime Before function invocation", zap.String("id", strings.TrimLeft(strings.TrimLeft(id, API_PREFIX), RTAPI_PREFIX)))
	}

	allAfterRtFunctions := make(map[string]Runtime2AfterRtFunction, len(goAfterRtFunctions)+len(luaAfterRtFunctions))
	for id, fn := range luaAfterRtFunctions {
		allAfterRtFunctions[id] = fn
		startupLogger.Info("Registered Lua runtime After function invocation", zap.String("id", strings.TrimLeft(strings.TrimLeft(id, API_PREFIX), RTAPI_PREFIX)))
	}
	for id, fn := range goAfterRtFunctions {
		allAfterRtFunctions[id] = fn
		startupLogger.Info("Registered Go runtime After function invocation", zap.String("id", strings.TrimLeft(strings.TrimLeft(id, API_PREFIX), RTAPI_PREFIX)))
	}

	var allMatchmakerMatchedFunction Runtime2MatchmakerMatchedFunction
	switch {
	case goMatchmakerMatchedFunction != nil:
		allMatchmakerMatchedFunction = goMatchmakerMatchedFunction
		startupLogger.Info("Registered Go runtime Matchmaker Matched function invocation")
	case luaMatchmakerMatchedFunction != nil:
		allMatchmakerMatchedFunction = luaMatchmakerMatchedFunction
		startupLogger.Info("Registered Lua runtime Matchmaker Matched function invocation")
	}

	return &Runtime2{
		providerGo:  goProvider,
		providerLua: luaProvider,

		rpcFunctions:              allRpcFunctions,
		beforeRtFunctions:         allBeforeRtFunctions,
		afterRtFunctions:          allAfterRtFunctions,
		matchmakerMatchedFunction: allMatchmakerMatchedFunction,
	}, nil
}

func (r *Runtime2) Rpc(id string) Runtime2RpcFunction {
	return r.rpcFunctions[id]
}

func (r *Runtime2) BeforeRt(id string) Runtime2BeforeRtFunction {
	return r.beforeRtFunctions[id]
}

func (r *Runtime2) AfterRt(id string) Runtime2AfterRtFunction {
	return r.afterRtFunctions[id]
}

func (r *Runtime2) MatchmakerMatched() Runtime2MatchmakerMatchedFunction {
	return r.matchmakerMatchedFunction
}
