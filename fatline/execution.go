package fatline

import (
	"errors"
	"fmt"

	"github.com/dash-xd/ratelimiter/redisacl"
)

// RedisExecutionIdentity is local process-enforcement material derived from a
// durable Fatline binding. It is not a global principal and must not be exposed
// as the caller identity outside the Redis execution boundary.
type RedisExecutionIdentity struct {
	Tenant         string
	Profile        string
	Username       string
	Rules          []string
	KeyPrefix      string
	ChannelPrefix  string
	FunctionPrefix string
}

// CompileRedisExecution turns the binding's local Redis execution profile into
// concrete ACL rules. Semantic identity/authz remains Prajapati's responsibility.
func CompileRedisExecution(binding Binding, compiler redisacl.Compiler, username, password string) (RedisExecutionIdentity, error) {
	if err := binding.Validate(); err != nil {
		return RedisExecutionIdentity{}, err
	}
	if compiler == nil {
		return RedisExecutionIdentity{}, errors.New("redis ACL compiler is required")
	}
	if binding.RedisACLProfile == "" {
		return RedisExecutionIdentity{}, errors.New("binding has no redis_acl_profile")
	}
	policy, err := compiler.Policy(binding.RedisACLProfile)
	if err != nil {
		return RedisExecutionIdentity{}, fmt.Errorf("resolve redis ACL profile: %w", err)
	}
	scope, err := compiler.Scope(binding.TenantID, username)
	if err != nil {
		return RedisExecutionIdentity{}, fmt.Errorf("resolve redis ACL scope: %w", err)
	}
	rules, err := compiler.Rules(redisacl.UserSpec{
		Tenant:   binding.TenantID,
		Username: scope.Username,
		Password: password,
		Policy:   policy,
		Reset:    true,
	})
	if err != nil {
		return RedisExecutionIdentity{}, fmt.Errorf("compile redis ACL rules: %w", err)
	}
	return RedisExecutionIdentity{
		Tenant:         binding.TenantID,
		Profile:        policy.Name,
		Username:       scope.Username,
		Rules:          rules,
		KeyPrefix:      scope.KeyPrefix,
		ChannelPrefix:  scope.ChannelPrefix,
		FunctionPrefix: scope.FunctionPrefix,
	}, nil
}
