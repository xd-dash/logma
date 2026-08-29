package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"
)

type groupCreateRequest struct {
	GroupID  string            `json:"groupID,omitempty"`
	IDs      []string          `json:"ids,omitempty"`
	Channels []json.RawMessage `json:"channels,omitempty"`
	Members  []json.RawMessage `json:"members,omitempty"`
}

type groupInlineMember struct {
	ID      string          `json:"id,omitempty"`
	Channel string          `json:"channel,omitempty"`
	callbackInput
}

func saveActiveChannelsGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	ids := append(
		[]string(nil),
		r.URL.Query()["id"]...,
	)

	all := strings.EqualFold(
		strings.TrimSpace(r.URL.Query().Get("all")),
		"true",
	)

	groupID := strings.TrimSpace(
		r.URL.Query().Get("groupID"),
	)

	if len(ids) == 0 && !all {
		bodyIDs, bodyGroupID, err := decodeSaveIDsBody(r)
		if err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}
		ids = bodyIDs
		if groupID == "" {
			groupID = bodyGroupID
		}
	}

	if !all && len(ids) == 0 {
		http.Error(
			w,
			"Provide one or more id query parameters, a body of channel ids, or all=true",
			http.StatusBadRequest,
		)
		return
	}

	if groupID == "" {
		groupID = generateGroupID()
	}

	var members []subscriptionGroupMember
	var err error

	if all {
		members, err = readActiveChannelsByIDs(nil)
	} else {
		members, err = readActiveChannelsByIDs(ids)
	}
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if err := writeSubscriptionGroupMembers(
		groupID,
		members,
	); err != nil {
		http.Error(
			w,
			"Error saving subscription group",
			http.StatusInternalServerError,
		)
		return
	}

	writeSubscriptionGroupResponse(
		w,
		http.StatusCreated,
		groupID,
		members,
	)
}

func decodeSaveIDsBody(
	r *http.Request,
) ([]string, string, error) {
	if r.Body == nil {
		return nil, "", nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, "", err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, "", nil
	}

	var ids []string
	if err := json.Unmarshal(body, &ids); err == nil {
		return ids, "", nil
	}

	var request struct {
		GroupID string   `json:"groupID,omitempty"`
		IDs     []string `json:"ids,omitempty"`
	}
	if err := json.Unmarshal(body, &request); err == nil {
		return request.IDs, request.GroupID, nil
	}

	return nil, "", errors.New(
		"body must be a JSON list of channel ids or an object with ids",
	)
}

func createSubscriptionGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request groupCreateRequest

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		if errors.Is(err, io.EOF) {
			http.Error(
				w,
				"Request body is required",
				http.StatusBadRequest,
			)
			return
		}

		http.Error(
			w,
			"Failed to parse request body",
			http.StatusBadRequest,
		)
		return
	}

	groupID := strings.TrimSpace(request.GroupID)
	if groupID == "" {
		groupID = generateGroupID()
	}

	members := make(
		[]subscriptionGroupMember,
		0,
		len(request.IDs)+
			len(request.Channels)+
			len(request.Members),
	)

	if len(request.IDs) > 0 {
		active, err := readActiveChannelsByIDs(
			request.IDs,
		)
		if err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}
		members = append(members, active...)
	}

	rawMembers := append(
		append(
			[]json.RawMessage(nil),
			request.Channels...,
		),
		request.Members...,
	)

	for _, raw := range rawMembers {
		member, err := decodeGroupMember(raw)
		if err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}

		if member.Channel == "" &&
			member.SubscriptionID != "" {
			active, err := readActiveChannelsByIDs(
				[]string{member.SubscriptionID},
			)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			members = append(members, active...)
			continue
		}

		members = append(members, member)
	}

	if len(members) == 0 {
		http.Error(
			w,
			"Subscription group requires at least one member",
			http.StatusBadRequest,
		)
		return
	}

	if err := writeSubscriptionGroupMembers(
		groupID,
		members,
	); err != nil {
		http.Error(
			w,
			"Error saving subscription group",
			http.StatusInternalServerError,
		)
		return
	}

	writeSubscriptionGroupResponse(
		w,
		http.StatusCreated,
		groupID,
		members,
	)
}

func decodeGroupMember(
	raw json.RawMessage,
) (subscriptionGroupMember, error) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil {
		id = strings.TrimSpace(id)
		if id == "" {
			return subscriptionGroupMember{},
				errors.New("channel id cannot be empty")
		}

		return subscriptionGroupMember{
			SubscriptionID: id,
		}, nil
	}

	var input groupInlineMember
	if err := json.Unmarshal(raw, &input); err != nil {
		return subscriptionGroupMember{},
			errors.New(
				"group members must be active channel ids or member objects",
			)
	}

	id = strings.TrimSpace(input.ID)
	channel := strings.TrimSpace(input.Channel)

	if channel == "" {
		if id == "" {
			return subscriptionGroupMember{},
				errors.New(
					"group member requires channel or active id",
				)
		}
		return subscriptionGroupMember{
			SubscriptionID: id,
		}, nil
	}

	callbacks, err := callbackConfigFromInput(
		input.callbackInput,
	)
	if err != nil {
		return subscriptionGroupMember{}, err
	}
	if callbackConfigEmpty(callbacks) {
		return subscriptionGroupMember{},
			errors.New(
				"inline group member requires callback configuration",
			)
	}

	if id == "" {
		id = generateGroupID()
	}

	return subscriptionGroupMember{
		SubscriptionID: id,
		Channel:        channel,
		Callbacks:      callbacks.Callbacks,
	}, nil
}

func readActiveChannelsByIDs(
	ids []string,
) ([]subscriptionGroupMember, error) {
	var keys []string
	var err error

	if len(ids) == 0 {
		keys, err = scanKeys(
			fmt.Sprintf(
				activeSubscriptionsPattern,
				"*",
				"*",
			),
		)
		if err != nil {
			return nil, err
		}
	} else {
		seen := make(map[string]struct{})

		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}

			found, err := scanKeys(
				fmt.Sprintf(
					activeSubscriptionsPattern,
					id,
					"*",
				),
			)
			if err != nil {
				return nil, err
			}
			if len(found) == 0 {
				return nil, fmt.Errorf(
					"active channel id %q not found",
					id,
				)
			}

			for _, key := range found {
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}

	members := make(
		[]subscriptionGroupMember,
		0,
		len(keys),
	)

	for _, key := range keys {
		stored, err := client.Get(
			rootCtx,
			key,
		).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}

		parts := strings.SplitN(
			key,
			":",
			3,
		)
		if len(parts) != 3 {
			continue
		}

		callbacks, err := decodeStoredCallbackConfig(
			stored,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"decode active channel %q callbacks: %w",
				parts[1],
				err,
			)
		}

		members = append(
			members,
			subscriptionGroupMember{
				SubscriptionID: parts[1],
				Channel:        parts[2],
				Callbacks:      callbacks.Callbacks,
			},
		)
	}

	return members, nil
}

func writeSubscriptionGroupMembers(
	groupID string,
	members []subscriptionGroupMember,
) error {
	for _, member := range members {
		callbacks, err := normalizeCallbackConfig(
			callbackConfig{
				Version:   callbackConfigVersion,
				Callbacks: member.Callbacks,
			},
		)
		if err != nil {
			return err
		}

		stored, err := encodeStoredCallbackConfig(
			callbacks,
		)
		if err != nil {
			return err
		}

		key := fmt.Sprintf(
			"%s:%s:%s:%s",
			subscriptionGroupsPrefix,
			groupID,
			member.SubscriptionID,
			member.Channel,
		)

		if err := client.Set(
			rootCtx,
			key,
			stored,
			0,
		).Err(); err != nil {
			return err
		}
	}

	return nil
}

func writeSubscriptionGroupResponse(
	w http.ResponseWriter,
	status int,
	groupID string,
	members []subscriptionGroupMember,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(
		subscriptionGroupResponse{
			GroupID: groupID,
			Members: members,
		},
	)
}
