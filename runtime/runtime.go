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

package runtime

import (
	"context"
	"database/sql"
	"github.com/heroiclabs/nakama/api"
	"github.com/heroiclabs/nakama/rtapi"
	"log"
)

const (
	RUNTIME_CTX_ENV              = "env"
	RUNTIME_CTX_MODE             = "execution_mode"
	RUNTIME_CTX_QUERY_PARAMS     = "query_params"
	RUNTIME_CTX_USER_ID          = "user_id"
	RUNTIME_CTX_USERNAME         = "username"
	RUNTIME_CTX_USER_SESSION_EXP = "user_session_exp"
	RUNTIME_CTX_SESSION_ID       = "session_id"
	RUNTIME_CTX_CLIENT_IP        = "client_ip"
	RUNTIME_CTX_CLIENT_PORT      = "client_port"
	RUNTIME_CTX_MATCH_ID         = "match_id"
	RUNTIME_CTX_MATCH_NODE       = "match_node"
	RUNTIME_CTX_MATCH_LABEL      = "match_label"
	RUNTIME_CTX_MATCH_TICK_RATE  = "match_tick_rate"
)

type Initialiser interface {
	RegisterRpc(id string, fn func(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, payload string) (string, error, int)) error
	RegisterBeforeRt(id string, fn func(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, envelope *rtapi.Envelope) (*rtapi.Envelope, error)) error
	RegisterAfterRt(id string, fn func(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, envelope *rtapi.Envelope) error) error
	RegisterMatchmakerMatched(fn func(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, entries []MatchmakerEntry) (string, error)) error
	RegisterMatch(name string, fn func(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule) (Match, error)) error
}

type PresenceMeta interface {
	GetHidden() bool
	GetPersistence() bool
	GetUsername() string
	GetStatus() string
}

type Presence interface {
	PresenceMeta
	GetUserId() string
	GetSessionId() string
	GetNodeId() string
}

type MatchmakerEntry interface {
	GetPresence() Presence
	GetTicket() string
	GetProperties() map[string]interface{}
}

type MatchData interface {
	Presence
	GetOpCode() int64
	GetData() []byte
	GetReceiveTime() int64
}

type MatchDispatcher interface {
	BroadcastMessage(opCode int64, data []byte, presences []Presence, sender Presence) error
	MatchKick(presences []Presence) error
	MatchLabelUpdate(label string) error
}

type Match interface {
	MatchInit(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, params map[string]interface{}) (interface{}, int, string)
	MatchJoinAttempt(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, dispatcher MatchDispatcher, tick int64, state interface{}, presence Presence) (interface{}, bool, string)
	MatchJoin(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, dispatcher MatchDispatcher, tick int64, state interface{}, presences []Presence) interface{}
	MatchLeave(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, dispatcher MatchDispatcher, tick int64, state interface{}, presences []Presence) interface{}
	MatchLoop(ctx context.Context, logger *log.Logger, db *sql.DB, nk NakamaModule, dispatcher MatchDispatcher, tick int64, state interface{}, messages []MatchData) interface{}
}

type NotificationSend struct {
	UserID     string
	Subject    string
	Content    map[string]interface{}
	Code       int
	Sender     string
	Persistent bool
}

type WalletUpdate struct {
	UserID    string
	Changeset map[string]interface{}
	Metadata  map[string]interface{}
}

type WalletLedgerItem interface {
	GetID() string
	GetUserID() string
	GetCreateTime() int64
	GetUpdateTime() int64
	GetChangeset() map[string]interface{}
	GetMetadata() map[string]interface{}
}

type StorageRead struct {
	Collection string
	Key        string
	UserID     string
}

type StorageWrite struct {
	Collection      string
	Key             string
	UserID          string
	Value           string
	Version         string
	PermissionRead  int
	PermissionWrite int
}

type StorageDelete struct {
	Collection string
	Key        string
	UserID     string
	Version    string
}

type NakamaModule interface {
	AuthenticateCustom(id, username string, create bool) (string, string, bool, error)
	AuthenticateDevice(id, username string, create bool) (string, string, bool, error)
	AuthenticateEmail(email, password, username string, create bool) (string, string, bool, error)
	AuthenticateFacebook(token string, importFriends bool, username string, create bool) (string, string, bool, error)
	AuthenticateGameCenter(playerID, bundleID string, timestamp int64, salt, signature, publicKeyUrl, username string, create bool) (string, string, bool, error)
	AuthenticateGoogle(token, username string, create bool) (string, string, bool, error)
	AuthenticateSteam(token, username string, create bool) (string, string, bool, error)

	AuthenticateTokenGenerate(userID, username string, exp int64) (string, error)

	AccountGetId(userID string) (*api.Account, error)
	// TODO nullable fields?
	AccountUpdateId(userID, username string, metadata map[string]interface{}, displayName, timezone, location, langTag, avatarUrl string) error

	UsersGetId(userIDs []string) ([]*api.User, error)
	UsersGetUsername(usernames []string) ([]*api.User, error)
	UsersBanId(userIDs []string) error
	UsersUnbanId(userIDs []string) error

	StreamUserList(mode uint8, subject, descriptor, label string, includeHidden, includeNotHidden bool) ([]Presence, error)
	StreamUserGet(mode uint8, subject, descriptor, label, userID, sessionID string) (PresenceMeta, error)
	StreamUserJoin(mode uint8, subject, descriptor, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error)
	StreamUserUpdate(mode uint8, subject, descriptor, label, userID, sessionID string, hidden, persistence bool, status string) error
	StreamUserLeave(mode uint8, subject, descriptor, label, userID, sessionID string) error
	StreamCount(mode uint8, subject, descriptor, label string) (int, error)
	StreamClose(mode uint8, subject, descriptor, label string) error
	StreamSend(mode uint8, subject, descriptor, label, data string) error

	MatchCreate(module string, params map[string]interface{}) (string, error)
	MatchList(limit int, authoritative bool, label string, minSize, maxSize int) []*api.Match

	NotificationSend(userID, subject string, content map[string]interface{}, code int, sender string, persistent bool) error
	NotificationsSend(notifications []*NotificationSend) error

	WalletUpdate(userID string, changeset, metadata map[string]interface{}) error
	WalletsUpdate(updates []*WalletUpdate) error
	WalletLedgerUpdate(itemID string, metadata map[string]interface{}) (WalletLedgerItem, error)
	WalletLedgerList(userID string) ([]WalletLedgerItem, error)

	StorageList(userID, collection string, limit int, cursor string) ([]*api.StorageObject, string, error)
	StorageRead(reads []*StorageRead) ([]*api.StorageObject, error)
	StorageWrite(writes []*StorageWrite) ([]*api.StorageObjectAck, error)
	StorageDelete(deletes []*StorageDelete) error

	LeaderboardCreate(id string, authoritative bool, sortOrder, operator, resetSchedule string, metadata map[string]interface{}) error
	LeaderboardDelete(id string) error
	LeaderboardRecordsList(id string, ownerIDs []string, limit int, cursor string) ([]*api.LeaderboardRecord, []*api.LeaderboardRecord, string, string, error)
	LeaderboardRecordWrite(id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*api.LeaderboardRecord, error)
	LeaderboardRecordDelete(id, ownerID string) error

	GroupCreate(userID, name, creatorID, langTag, description, avatarUrl string, open bool, metadata map[string]interface{}, maxCount int) (*api.Group, error)
	GroupUpdate(id, name, creatorID, langTag, description, avatarUrl string, open bool, metadata map[string]interface{}, maxCount int) error
	GroupDelete(id string) error
	GroupUsersList(id string) ([]*api.GroupUserList_GroupUser, error)
	UserGroupsList(userID string) ([]*api.UserGroupList_UserGroup, error)
}
