package base

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
)

func Hash(prefix string, o interface{}) uint64 {
	h := fnv.New64()
	h.Write([]byte(prefix))
	h.Write([]byte(fmt.Sprintf("%v", o)))
	return h.Sum64()
}

type entry struct {
	key   uint64
	value Message
}

func hashSort(messages []Message) {
	entries := make([]entry, 0, len(messages))
	for _, message := range messages {
		entries = append(entries, entry{
			key:   message.Hash(),
			value: message,
		})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	for i := range messages {
		messages[i] = entries[i].value
	}
}

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}

	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
