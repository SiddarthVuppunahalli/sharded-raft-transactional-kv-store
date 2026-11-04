package multiraft

import "encoding/json"

// Command is the replicated unit applied to a range state machine.
// Keeping the command envelope independent of raftpb makes the application
// state machine portable across transport implementations.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func marshalCommand(command Command) ([]byte, error) {
	return json.Marshal(command)
}

func unmarshalCommand(data []byte) (Command, error) {
	var command Command
	err := json.Unmarshal(data, &command)
	return command, err
}
