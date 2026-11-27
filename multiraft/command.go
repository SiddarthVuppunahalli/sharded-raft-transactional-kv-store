package multiraft

import "encoding/json"

// Command is the replicated unit applied to a range state machine.
// Keeping the command envelope independent of raftpb makes the application
// state machine portable across transport implementations.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
	// ReqID lets the proposing node match a committed entry back to the
	// waiting client request. It is opaque to the state machine.
	ReqID string `json:"req_id,omitempty"`
}

func marshalCommand(command Command) ([]byte, error) {
	return json.Marshal(command)
}

func unmarshalCommand(data []byte) (Command, error) {
	var command Command
	err := json.Unmarshal(data, &command)
	return command, err
}
