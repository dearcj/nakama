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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/jsonpb"
	"github.com/heroiclabs/nakama/rtapi"
	"github.com/heroiclabs/nakama/social"
	"github.com/yuin/gopher-lua"
	"go.opencensus.io/stats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const LTSentinel = lua.LValueType(-1)

type LSentinelType struct {
	lua.LNilType
}

func (s *LSentinelType) String() string       { return "" }
func (s *LSentinelType) Type() lua.LValueType { return LTSentinel }

var LSentinel = lua.LValue(&LSentinelType{})

type RuntimeLuaModule struct {
	Name    string
	Path    string
	Content []byte
}

type RuntimeLuaModuleCache struct {
	Names   []string
	Modules map[string]*RuntimeLuaModule
}

func (mc *RuntimeLuaModuleCache) Add(m *RuntimeLuaModule) {
	mc.Names = append(mc.Names, m.Name)
	mc.Modules[m.Name] = m

	// Ensure modules will be listed in ascending order of names.
	sort.Strings(mc.Names)
}

type RuntimeProviderLua struct {
	sync.Mutex
	logger            *zap.Logger
	jsonpbMarshaler   *jsonpb.Marshaler
	jsonpbUnmarshaler *jsonpb.Unmarshaler

	poolCh       chan *RuntimeLua
	maxCount     int
	currentCount int
	newFn        func() *RuntimeLua

	statsCtx context.Context
}

func NewRuntimeProviderLua(logger, startupLogger *zap.Logger, db *sql.DB, jsonpbMarshaler *jsonpb.Marshaler, jsonpbUnmarshaler *jsonpb.Unmarshaler, config Config, socialClient *social.Client, leaderboardCache LeaderboardCache, sessionRegistry *SessionRegistry, matchRegistry MatchRegistry, tracker Tracker, router MessageRouter, rootPath string, paths []string) ([]string, map[string]Runtime2RpcFunction, map[string]Runtime2BeforeRtFunction, map[string]Runtime2AfterRtFunction, Runtime2MatchmakerMatchedFunction, RuntimeProvider, error) {
	moduleCache := &RuntimeLuaModuleCache{
		Names:   make([]string, 0),
		Modules: make(map[string]*RuntimeLuaModule, 0),
	}
	modulePaths := make([]string, 0)

	// Override before Package library is invoked.
	lua.LuaLDir = rootPath
	lua.LuaPathDefault = lua.LuaLDir + string(os.PathSeparator) + "?.lua;" + lua.LuaLDir + string(os.PathSeparator) + "?" + string(os.PathSeparator) + "init.lua"
	os.Setenv(lua.LuaPath, lua.LuaPathDefault)

	startupLogger.Info("Initialising Lua runtime provider", zap.String("path", lua.LuaLDir))

	for _, path := range paths {
		if strings.ToLower(filepath.Ext(path)) != ".lua" {
			continue
		}

		// Load the file contents into memory.
		var content []byte
		var err error
		if content, err = ioutil.ReadFile(path); err != nil {
			startupLogger.Error("Could not read Lua module", zap.String("path", path), zap.Error(err))
			return nil, nil, nil, nil, nil, nil, err
		}

		relPath, _ := filepath.Rel(rootPath, path)
		name := strings.TrimSuffix(relPath, filepath.Ext(relPath))
		// Make paths Lua friendly.
		name = strings.Replace(name, string(os.PathSeparator), ".", -1)

		moduleCache.Add(&RuntimeLuaModule{
			Name:    name,
			Path:    path,
			Content: content,
		})
		modulePaths = append(modulePaths, relPath)
	}

	stdLibs := map[string]lua.LGFunction{
		lua.LoadLibName:   OpenPackage2(moduleCache),
		lua.BaseLibName:   lua.OpenBase,
		lua.TabLibName:    lua.OpenTable,
		lua.OsLibName:     OpenOs,
		lua.StringLibName: lua.OpenString,
		lua.MathLibName:   lua.OpenMath,
		Bit32LibName:      OpenBit32,
	}
	once := &sync.Once{}
	rpcFunctions := make(map[string]Runtime2RpcFunction, 0)
	beforeRtFunctions := make(map[string]Runtime2BeforeRtFunction, 0)
	afterRtFunctions := make(map[string]Runtime2AfterRtFunction, 0)
	var matchmakerMatchedFunction Runtime2MatchmakerMatchedFunction

	runtimeProviderLua := &RuntimeProviderLua{
		logger:            logger,
		jsonpbMarshaler:   jsonpbMarshaler,
		jsonpbUnmarshaler: jsonpbUnmarshaler,

		poolCh:   make(chan *RuntimeLua, config.GetRuntime().MaxCount),
		maxCount: config.GetRuntime().MaxCount,
		// Set the current count assuming we'll warm up the pool in a moment.
		currentCount: config.GetRuntime().MinCount,
		newFn: func() *RuntimeLua {
			r, err := newRuntimeLuaVM(logger, db, config, socialClient, leaderboardCache, sessionRegistry, matchRegistry, tracker, router, stdLibs, moduleCache, once, nil)
			if err != nil {
				logger.Fatal("Failed to initialize Lua runtime", zap.Error(err))
			}
			return r
		},

		statsCtx: context.Background(),
	}

	startupLogger.Info("Evaluating Lua runtime modules")

	r, err := newRuntimeLuaVM(logger, db, config, socialClient, leaderboardCache, sessionRegistry, matchRegistry, tracker, router, stdLibs, moduleCache, once, func(execMode RuntimeExecutionMode, id string) {
		switch execMode {
		case RuntimeExecutionModeRPC:
			rpcFunctions[id] = func(queryParams map[string][]string, userID, username string, expiry int64, sessionID, clientIP, clientPort, payload string) (string, error, codes.Code) {
				return runtimeProviderLua.Rpc(id, queryParams, userID, username, expiry, sessionID, clientIP, clientPort, payload)
			}
		case RuntimeExecutionModeBefore:
			if strings.HasPrefix(strings.ToLower(RTAPI_PREFIX), id) {
				beforeRtFunctions[id] = func(logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) (*rtapi.Envelope, error) {
					return runtimeProviderLua.BeforeRt(id, logger, userID, username, expiry, sessionID, clientIP, clientPort, envelope)
				}
			}
			// TODO beforeReq
		case RuntimeExecutionModeAfter:
			if strings.HasPrefix(strings.ToLower(RTAPI_PREFIX), id) {
				afterRtFunctions[id] = func(logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) error {
					return runtimeProviderLua.AfterRt(id, logger, userID, username, expiry, sessionID, clientIP, clientPort, envelope)
				}
			}
			// TODO afterReq
		case RuntimeExecutionModeMatchmaker:
			matchmakerMatchedFunction = func(entries []*MatchmakerEntry) (string, error) {
				return runtimeProviderLua.MatchmakerMatched(entries)
			}
		}
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	r.Stop()

	startupLogger.Info("Lua runtime modules loaded")

	// Warm up the pool.
	startupLogger.Info("Allocating minimum runtime pool", zap.Int("count", runtimeProviderLua.currentCount))
	if len(moduleCache.Names) > 0 {
		// Only if there are runtime modules to load.
		for i := 0; i < config.GetRuntime().MinCount; i++ {
			runtimeProviderLua.poolCh <- runtimeProviderLua.newFn()
		}
		stats.Record(runtimeProviderLua.statsCtx, MetricsRuntimeCount.M(int64(config.GetRuntime().MinCount)))
	}
	startupLogger.Info("Allocated minimum runtime pool")

	return modulePaths, rpcFunctions, beforeRtFunctions, afterRtFunctions, matchmakerMatchedFunction, runtimeProviderLua, nil
}

func (rp *RuntimeProviderLua) Rpc(id string, queryParams map[string][]string, userID, username string, expiry int64, sessionID, clientIP, clientPort, payload string) (string, error, codes.Code) {
	runtime := rp.Get()
	lf := runtime.GetCallback(RuntimeExecutionModeRPC, id)
	if lf == nil {
		rp.Put(runtime)
		return "", ErrRuntimeRPCNotFound, codes.NotFound
	}

	result, fnErr, code := runtime.InvokeFunction(RuntimeExecutionModeRPC, lf, queryParams, userID, username, expiry, sessionID, clientIP, clientPort, payload)
	rp.Put(runtime)

	if fnErr != nil {
		rp.logger.Error("Runtime RPC function caused an error", zap.String("id", id), zap.Error(fnErr))
		if apiErr, ok := fnErr.(*lua.ApiError); ok && !rp.logger.Core().Enabled(zapcore.InfoLevel) {
			msg := apiErr.Object.String()
			if strings.HasPrefix(msg, lf.Proto.SourceName) {
				msg = msg[len(lf.Proto.SourceName):]
				msgParts := strings.SplitN(msg, ": ", 2)
				if len(msgParts) == 2 {
					msg = msgParts[1]
				} else {
					msg = msgParts[0]
				}
			}
			return "", errors.New(msg), code
		} else {
			return "", fnErr, code
		}
	}

	if result == nil {
		return "", nil, 0
	}

	if payload, ok := result.(string); !ok {
		rp.logger.Warn("Lua runtime function returned invalid data", zap.Any("result", result))
		return "", errors.New("Runtime function returned invalid data - only allowed one return value of type String/Byte."), codes.Internal
	} else {
		return payload, nil, 0
	}
}

func (rp *RuntimeProviderLua) BeforeRt(id string, logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) (*rtapi.Envelope, error) {
	runtime := rp.Get()
	lf := runtime.GetCallback(RuntimeExecutionModeBefore, id)
	if lf == nil {
		rp.Put(runtime)
		return nil, errors.New("Runtime Before function not found.")
	}

	envelopeJSON, err := rp.jsonpbMarshaler.MarshalToString(envelope)
	if err != nil {
		rp.Put(runtime)
		logger.Error("Could not marshall envelope to JSON", zap.Any("envelope", envelope), zap.Error(err))
		return nil, errors.New("Could not run runtime Before function.")
	}
	var envelopeMap map[string]interface{}
	if err := json.Unmarshal([]byte(envelopeJSON), &envelopeMap); err != nil {
		rp.Put(runtime)
		logger.Error("Could not unmarshall envelope to interface{}", zap.Any("envelope_json", envelopeJSON), zap.Error(err))
		return nil, errors.New("Could not run runtime Before function.")
	}

	result, fnErr, _ := runtime.InvokeFunction(RuntimeExecutionModeBefore, lf, nil, userID, username, expiry, sessionID, clientIP, clientPort, envelopeMap)
	rp.Put(runtime)

	if fnErr != nil {
		logger.Error("Runtime Before function caused an error.", zap.String("id", id), zap.Error(fnErr))
		if apiErr, ok := fnErr.(*lua.ApiError); ok && !logger.Core().Enabled(zapcore.InfoLevel) {
			msg := apiErr.Object.String()
			if strings.HasPrefix(msg, lf.Proto.SourceName) {
				msg = msg[len(lf.Proto.SourceName):]
				msgParts := strings.SplitN(msg, ": ", 2)
				if len(msgParts) == 2 {
					msg = msgParts[1]
				} else {
					msg = msgParts[0]
				}
			}
			return nil, errors.New(msg)
		} else {
			return nil, fnErr
		}
	}

	if result == nil {
		return nil, nil
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		logger.Error("Could not marshall result to JSON", zap.Any("result", result), zap.Error(err))
		return nil, errors.New("Could not complete runtime Before function.")
	}

	if err = rp.jsonpbUnmarshaler.Unmarshal(strings.NewReader(string(resultJSON)), envelope); err != nil {
		logger.Error("Could not unmarshall result to envelope", zap.Any("result", result), zap.Error(err))
		return nil, errors.New("Could not complete runtime Before function.")
	}

	return envelope, nil
}

func (rp *RuntimeProviderLua) AfterRt(id string, logger *zap.Logger, userID, username string, expiry int64, sessionID, clientIP, clientPort string, envelope *rtapi.Envelope) error {
	runtime := rp.Get()
	lf := runtime.GetCallback(RuntimeExecutionModeAfter, id)
	if lf == nil {
		rp.Put(runtime)
		return errors.New("Runtime After function not found.")
	}

	envelopeJSON, err := rp.jsonpbMarshaler.MarshalToString(envelope)
	if err != nil {
		rp.Put(runtime)
		logger.Error("Could not marshall envelope to JSON", zap.Any("envelope", envelope), zap.Error(err))
		return errors.New("Could not run runtime After function.")
	}
	var envelopeMap map[string]interface{}
	if err := json.Unmarshal([]byte(envelopeJSON), &envelopeMap); err != nil {
		rp.Put(runtime)
		logger.Error("Could not unmarshall envelope to interface{}", zap.Any("envelope_json", envelopeJSON), zap.Error(err))
		return errors.New("Could not run runtime After function.")
	}

	_, fnErr, _ := runtime.InvokeFunction(RuntimeExecutionModeAfter, lf, nil, userID, username, expiry, sessionID, clientIP, clientPort, envelopeMap)
	rp.Put(runtime)

	if fnErr != nil {
		logger.Error("Runtime After function caused an error.", zap.String("id", id), zap.Error(fnErr))
		if apiErr, ok := fnErr.(*lua.ApiError); ok && !logger.Core().Enabled(zapcore.InfoLevel) {
			msg := apiErr.Object.String()
			if strings.HasPrefix(msg, lf.Proto.SourceName) {
				msg = msg[len(lf.Proto.SourceName):]
				msgParts := strings.SplitN(msg, ": ", 2)
				if len(msgParts) == 2 {
					msg = msgParts[1]
				} else {
					msg = msgParts[0]
				}
			}
			return errors.New(msg)
		} else {
			return fnErr
		}
	}

	return nil
}

func (rp *RuntimeProviderLua) MatchmakerMatched(entries []*MatchmakerEntry) (string, error) {
	runtime := rp.Get()
	lf := runtime.GetCallback(RuntimeExecutionModeMatchmaker, "")
	if lf == nil {
		rp.Put(runtime)
		return "", errors.New("Runtime Matchmaker Matched function not found.")
	}

	ctx := NewLuaContext(runtime.vm, runtime.luaEnv, ExecutionModeMatchmaker, nil, 0, "", "", "", "", "")

	entriesTable := runtime.vm.CreateTable(len(entries), 0)
	for i, entry := range entries {
		presenceTable := runtime.vm.CreateTable(0, 4)
		presenceTable.RawSetString("user_id", lua.LString(entry.Presence.UserId))
		presenceTable.RawSetString("session_id", lua.LString(entry.Presence.SessionId))
		presenceTable.RawSetString("username", lua.LString(entry.Presence.Username))
		presenceTable.RawSetString("node", lua.LString(entry.Presence.Node))

		propertiesTable := runtime.vm.CreateTable(0, len(entry.StringProperties)+len(entry.NumericProperties))
		for k, v := range entry.StringProperties {
			propertiesTable.RawSetString(k, lua.LString(v))
		}
		for k, v := range entry.NumericProperties {
			propertiesTable.RawSetString(k, lua.LNumber(v))
		}

		entryTable := runtime.vm.CreateTable(0, 2)
		entryTable.RawSetString("presence", presenceTable)
		entryTable.RawSetString("properties", propertiesTable)

		entriesTable.RawSetInt(i+1, entryTable)
	}

	retValue, err, _ := runtime.invokeFunction(runtime.vm, lf, ctx, entriesTable)
	rp.Put(runtime)
	if err != nil {
		return "", fmt.Errorf("Error running runtime Matchmaker Matched hook: %v", err.Error())
	}

	if retValue == nil || retValue == lua.LNil {
		// No return value or hook decided not to return an authoritative match ID.
		return "", nil
	}

	if retValue.Type() == lua.LTString {
		// Hook (maybe) returned an authoritative match ID.
		matchIDString := retValue.String()

		// Validate the match ID.
		matchIDComponents := strings.SplitN(matchIDString, ".", 2)
		if len(matchIDComponents) != 2 {
			return "", errors.New("Invalid return value from runtime Matchmaker Matched hook, not a valid match ID.")
		}
		_, err = uuid.FromString(matchIDComponents[0])
		if err != nil {
			return "", errors.New("Invalid return value from runtime Matchmaker Matched hook, not a valid match ID.")
		}

		return matchIDString, nil
	}

	return "", errors.New("Unexpected return type from runtime Matchmaker Matched hook, must be string or nil.")
}

func (rp *RuntimeProviderLua) Get() *RuntimeLua {
	select {
	case r := <-rp.poolCh:
		// Ideally use an available idle runtime.
		return r
	default:
		// If there was no idle runtime, see if we can allocate a new one.
		rp.Lock()
		if rp.currentCount >= rp.maxCount {
			rp.Unlock()
			// If we've reached the max allowed allocation block on an available runtime.
			return <-rp.poolCh
		}
		// Inside the locked region now, last chance to use an available idle runtime.
		// Note: useful in case a runtime becomes available while waiting to acquire lock.
		select {
		case r := <-rp.poolCh:
			rp.Unlock()
			return r
		default:
			// Allocate a new runtime.
			rp.currentCount++
			rp.Unlock()
			stats.Record(rp.statsCtx, MetricsRuntimeCount.M(1))
			return rp.newFn()
		}
	}
}

func (rp *RuntimeProviderLua) Put(r *RuntimeLua) {
	select {
	case rp.poolCh <- r:
		// Runtime is successfully returned to the pool.
	default:
		// The pool is over capacity. Should never happen but guard anyway.
		// Safe to continue processing, the runtime is just discarded.
		rp.logger.Warn("Runtime pool full, discarding Lua runtime")
	}
}

type RuntimeLua struct {
	logger *zap.Logger
	vm     *lua.LState
	luaEnv *lua.LTable
}

func (r *RuntimeLua) loadModules(moduleCache *RuntimeLuaModuleCache) error {
	// `DoFile(..)` only parses and evaluates modules. Calling it multiple times, will load and eval the file multiple times.
	// So to make sure that we only load and evaluate modules once, regardless of whether there is dependency between files, we load them all into `preload`.
	// This is to make sure that modules are only loaded and evaluated once as `doFile()` does not (always) update _LOADED table.
	// Bear in mind two separate thoughts around the script runtime design choice:
	//
	// 1) This is only a problem if one module is dependent on another module.
	// This means that the global functions are evaluated once at system startup and then later on when the module is required through `require`.
	// We circumvent this by checking the _LOADED table to check if `require` had evaluated the module and avoiding double-eval.
	//
	// 2) Second item is that modules must be pre-loaded into the state for callback-func eval to work properly (in case of HTTP/RPC/etc invokes)
	// So we need to always load the modules into the system via `preload` so that they are always available in the LState.
	// We can't rely on `require` to have seen the module in case there is no dependency between the modules.

	//for _, mod := range r.modules {
	//	relPath, _ := filepath.Rel(r.luaPath, mod)
	//	moduleName := strings.TrimSuffix(relPath, filepath.Ext(relPath))
	//
	//	// check to see if this module was loaded by `require` before executing it
	//	loaded := l.GetField(l.Get(lua.RegistryIndex), "_LOADED")
	//	lv := l.GetField(loaded, moduleName)
	//	if lua.LVAsBool(lv) {
	//		// Already evaluated module via `require(..)`
	//		continue
	//	}
	//
	//	if err = l.DoFile(mod); err != nil {
	//		failedModules++
	//		r.logger.Error("Failed to evaluate module - skipping", zap.String("path", mod), zap.Error(err))
	//	}
	//}

	preload := r.vm.GetField(r.vm.GetField(r.vm.Get(lua.EnvironIndex), "package"), "preload")
	fns := make(map[string]*lua.LFunction)
	for _, name := range moduleCache.Names {
		module, ok := moduleCache.Modules[name]
		if !ok {
			r.logger.Fatal("Failed to find named module in cache", zap.String("name", name))
		}
		f, err := r.vm.Load(bytes.NewReader(module.Content), module.Path)
		if err != nil {
			r.logger.Error("Could not load module", zap.String("name", module.Path), zap.Error(err))
			return err
		} else {
			r.vm.SetField(preload, module.Name, f)
			fns[module.Name] = f
		}
	}

	for _, name := range moduleCache.Names {
		fn, ok := fns[name]
		if !ok {
			r.logger.Fatal("Failed to find named module in prepared functions", zap.String("name", name))
		}
		loaded := r.vm.GetField(r.vm.Get(lua.RegistryIndex), "_LOADED")
		lv := r.vm.GetField(loaded, name)
		if lua.LVAsBool(lv) {
			// Already evaluated module via `require(..)`
			continue
		}

		r.vm.Push(fn)
		fnErr := r.vm.PCall(0, -1, nil)
		if fnErr != nil {
			r.logger.Error("Could not complete runtime invocation", zap.Error(fnErr))
			return fnErr
		}
	}

	return nil
}

func (r *RuntimeLua) NewStateThread() (*lua.LState, context.CancelFunc) {
	return r.vm, nil
}

func (r *RuntimeLua) GetCallback(e RuntimeExecutionMode, key string) *lua.LFunction {
	cp := r.vm.Context().Value(RUNTIME_LUA_CALLBACKS).(*RuntimeLuaCallbacks)
	switch e {
	case RuntimeExecutionModeRPC:
		return cp.RPC[key]
	case RuntimeExecutionModeBefore:
		return cp.Before[key]
	case RuntimeExecutionModeAfter:
		return cp.After[key]
	case RuntimeExecutionModeMatchmaker:
		return cp.Matchmaker
	}

	return nil
}

func (r *RuntimeLua) InvokeFunction(execMode RuntimeExecutionMode, fn *lua.LFunction, queryParams map[string][]string, uid string, username string, sessionExpiry int64, sid string, clientIP string, clientPort string, payload interface{}) (interface{}, error, codes.Code) {
	ctx := NewRuntimeLuaContext(r.vm, r.luaEnv, execMode, queryParams, sessionExpiry, uid, username, sid, clientIP, clientPort)
	var lv lua.LValue
	if payload != nil {
		lv = RuntimeLuaConvertValue(r.vm, payload)
	}

	retValue, err, code := r.invokeFunction(r.vm, fn, ctx, lv)
	if err != nil {
		return nil, err, code
	}

	if retValue == nil || retValue == lua.LNil {
		return nil, nil, 0
	}

	return RuntimeLuaConvertLuaValue(retValue), nil, 0
}

func (r *RuntimeLua) invokeFunction(l *lua.LState, fn *lua.LFunction, ctx *lua.LTable, payload lua.LValue) (lua.LValue, error, codes.Code) {
	l.Push(LSentinel)
	l.Push(fn)

	nargs := 1
	l.Push(ctx)

	if payload != nil {
		nargs = 2
		l.Push(payload)
	}

	err := l.PCall(nargs, lua.MultRet, nil)
	if err != nil {
		// Unwind the stack up to and including our sentinel value, effectively discarding any other returned parameters.
		for {
			v := l.Get(-1)
			l.Pop(1)
			if v.Type() == LTSentinel {
				break
			}
		}

		if apiError, ok := err.(*lua.ApiError); ok && apiError.Object.Type() == lua.LTTable {
			t := apiError.Object.(*lua.LTable)
			switch t.Len() {
			case 0:
				return nil, err, codes.Internal
			case 1:
				apiError.Object = t.RawGetInt(1)
				return nil, err, codes.Internal
			default:
				// Ignore everything beyond the first 2 params, if there are more.
				apiError.Object = t.RawGetInt(1)
				code := codes.Internal
				if c := t.RawGetInt(2); c.Type() == lua.LTNumber {
					code = codes.Code(c.(lua.LNumber))
				}
				return nil, err, code
			}
		}

		return nil, err, codes.Internal
	}

	retValue := l.Get(-1)
	l.Pop(1)
	if retValue.Type() == LTSentinel {
		return nil, nil, 0
	}

	// Unwind the stack up to and including our sentinel value, effectively discarding any other returned parameters.
	for {
		v := l.Get(-1)
		l.Pop(1)
		if v.Type() == LTSentinel {
			break
		}
	}

	return retValue, nil, 0
}

func (r *RuntimeLua) Stop() {
	// Not necessarily required as it only does OS temp files cleanup, which we don't expose in the runtime.
	r.vm.Close()
}

func newRuntimeLuaVM(logger *zap.Logger, db *sql.DB, config Config, socialClient *social.Client, leaderboardCache LeaderboardCache, sessionRegistry *SessionRegistry, matchRegistry MatchRegistry, tracker Tracker, router MessageRouter, stdLibs map[string]lua.LGFunction, moduleCache *RuntimeLuaModuleCache, once *sync.Once, announceCallback func(RuntimeExecutionMode, string)) (*RuntimeLua, error) {
	// Initialize a one-off runtime to ensure startup code runs and modules are valid.
	vm := lua.NewState(lua.Options{
		CallStackSize:       config.GetRuntime().CallStackSize,
		RegistrySize:        config.GetRuntime().RegistrySize,
		SkipOpenLibs:        true,
		IncludeGoStackTrace: true,
	})
	for name, lib := range stdLibs {
		vm.Push(vm.NewFunction(lib))
		vm.Push(lua.LString(name))
		vm.Call(1, 0)
	}
	nakamaModule := NewRuntimeLuaNakamaModule(logger, db, config, socialClient, leaderboardCache, vm, sessionRegistry, matchRegistry, tracker, router, once, announceCallback)
	vm.PreloadModule("nakama", nakamaModule.Loader)
	r := &RuntimeLua{
		logger: logger,
		vm:     vm,
		luaEnv: RuntimeLuaConvertMapString(vm, config.GetRuntime().Environment),
	}

	return r, r.loadModules(moduleCache)
}
