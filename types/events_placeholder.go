package types

import (
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cosmos/gogoproto/proto"
)

var _ PublishEvent = (*AbciEventPlaceholder)(nil)

type AbciEventPlaceholder struct {
	typ string
}

func NewAbciEventPlaceholder(event Event) AbciEventPlaceholder {
	return AbciEventPlaceholder{event.Type}
}

func NewTypedEventPlaceholder(tev proto.Message) AbciEventPlaceholder {
	return AbciEventPlaceholder{proto.MessageName(tev)}
}

func (e AbciEventPlaceholder) ToString() string {
	return e.typ
}

func (e AbciEventPlaceholder) Serialize() []byte {
	return []byte(e.typ)
}

var _ EventManagerI = (*EventPlaceholderManager)(nil)

type EventPlaceholderManager struct {
	eventManager        EventManagerI
	publishEventManager PublishEventManagerI
}

func (e *EventPlaceholderManager) ABCIEvents() []abci.Event {
	return e.eventManager.ABCIEvents()
}

func (e *EventPlaceholderManager) EmitEvent(event Event) {
	e.publishEventManager.EmitEvent(NewAbciEventPlaceholder(event))
	e.eventManager.EmitEvent(event)
}

func (e *EventPlaceholderManager) EmitEvents(events Events) {
	wrappers := make(PublishEvents, len(events))
	for _, event := range events {
		wrappers = append(wrappers, NewAbciEventPlaceholder(event))
	}
	e.publishEventManager.EmitEvents(wrappers)
	e.eventManager.EmitEvents(events)
}

func (e *EventPlaceholderManager) EmitTypedEvent(tev proto.Message) error {
	e.publishEventManager.EmitEvent(NewTypedEventPlaceholder(tev))
	return e.eventManager.EmitTypedEvent(tev)
}

func (e *EventPlaceholderManager) EmitTypedEvents(tevs ...proto.Message) error {
	wrappers := make(PublishEvents, len(tevs))
	for _, tev := range tevs {
		wrappers = append(wrappers, NewTypedEventPlaceholder(tev))
	}
	e.publishEventManager.EmitEvents(wrappers)
	return e.eventManager.EmitTypedEvents(tevs...)
}

func (e *EventPlaceholderManager) Events() Events {
	return e.eventManager.Events()
}
