package base

const (
	Empty           = "empty"
	UnknownDest     = "unknown destination"
	Partition       = "partition"
	DropOff         = "drop off"
	Handle          = "handle"
	HandleDuplicate = "handle duplicate"
	Trigger         = "trigger"
)

// Event type is used when a new state is inherit from an old one.
type Event struct {
	Action   string
	Instance interface{}
}

func EmptyEvent() Event {
	return Event{
		Action:   Empty,
		Instance: nil,
	}
}

func UnknownDestinationEvent(m Message) Event {
	return Event{
		Action:   UnknownDest,
		Instance: m,
	}
}

func PartitionEvent(m Message) Event {
	return Event{
		Action:   Partition,
		Instance: m,
	}
}

func DropOffEvent(m Message) Event {
	return Event{
		Action:   DropOff,
		Instance: m,
	}
}

func HandleEvent(m Message) Event {
	return Event{
		Action:   Handle,
		Instance: m,
	}
}

func HandleDuplicateEvent(m Message) Event {
	return Event{
		Action:   HandleDuplicate,
		Instance: m,
	}
}

type TimerInstance struct {
	Address
	Timer
}

func TriggerEvent(addr Address, t Timer) Event {
	return Event{
		Action: Trigger,
		Instance: TimerInstance{
			Address: addr,
			Timer:   t,
		},
	}
}
