package sandbox

import "strings"

// RecursiveDirectArgv mirrors agentproc.buildRecursiveDirectArgs for the
// sandbox path. The host path builds its argv inside pkg/agentproc (the
// runner owns the spawn); the sandbox path hands argv to the Docker exec API,
// so the shape is reproduced here. Keep the two in lockstep — the argv is the
// Python runtime's recursive-direct contract:
//
//	recursive --workspace <path> --output-format <json|stream-json>
//	          --permission-mode auto [--model M] [--max-steps N]
//	          run <message> | resume <session> --message <message>
func RecursiveDirectArgv(env map[string]string, message, sessionID string) []string {
	bin := envOr(env, "RECURSIVE_BIN", "recursive")
	args := []string{
		bin,
		"--workspace", envOr(env, "RECURSIVE_WORKSPACE", "."),
		"--output-format", envOr(env, "RECURSIVE_OUTPUT_FORMAT", "json"),
		"--permission-mode", "auto",
	}
	if v := envOr(env, "RECURSIVE_MODEL", ""); v != "" {
		args = append(args, "--model", v)
	}
	if v := envOr(env, "RECURSIVE_MAX_STEPS", ""); v != "" {
		args = append(args, "--max-steps", v)
	}
	if sessionID != "" {
		args = append(args, "resume", sessionID, "--message", message)
	} else {
		args = append(args, "run", message)
	}
	return args
}

func envOr(env map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(env[key]); value != "" {
		return value
	}
	return fallback
}
