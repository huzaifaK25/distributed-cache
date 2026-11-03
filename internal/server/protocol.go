package server

import (
	"fmt"
	"strings"
)

// CommandType identifies a supported line-protocol command.
type CommandType string

const (
	CommandPing  CommandType = "PING"
	CommandSet   CommandType = "SET"
	CommandGet   CommandType = "GET"
	CommandDel   CommandType = "DEL"
	CommandStats CommandType = "STATS"
)

// Command is a parsed client request.
type Command struct {
	Type  CommandType
	Key   string
	Value string
}

// ParseCommand converts one line into a Command.
//
// The protocol is line-oriented: the command is case-insensitive, arguments are
// separated by spaces or tabs, and a SET value may contain spaces. Empty lines,
// unknown commands, and incorrect argument counts are rejected.
func ParseCommand(line string) (Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Command{}, fmt.Errorf("empty command")
	}

	token, args := splitFirstToken(line)
	switch commandType := CommandType(strings.ToUpper(token)); commandType {
	case CommandPing, CommandStats:
		if args != "" {
			return Command{}, fmt.Errorf("usage: %s", commandType)
		}
		return Command{Type: commandType}, nil

	case CommandGet, CommandDel:
		if args == "" || strings.ContainsAny(args, " \t") {
			return Command{}, fmt.Errorf("usage: %s <key>", commandType)
		}
		return Command{Type: commandType, Key: args}, nil

	case CommandSet:
		key, value := splitFirstToken(args)
		if key == "" || value == "" {
			return Command{}, fmt.Errorf("usage: SET <key> <value>")
		}
		return Command{Type: CommandSet, Key: key, Value: value}, nil

	default:
		return Command{}, fmt.Errorf("unknown command %q", token)
	}
}

// splitFirstToken splits s at its first space or tab and trims both sides.
// It returns an empty second string when no argument follows the token.
func splitFirstToken(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}

	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i], strings.TrimSpace(s[i:])
	}
	return s, ""
}
