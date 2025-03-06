package types

var _ PublishEventManagerI = (*EventPlaceholderManager)(nil)

type EventPlaceholderManager struct {
	eventManager        EventManagerI
	publishEventManager PublishEventManagerI
}

func (e *EventPlaceholderManager) Events() PublishEvents {
	return e.publishEventManager.Events()
}

func (e *EventPlaceholderManager) EmitEvent(event PublishEvent) {
	e.publishEventManager.EmitEvent(event)
	placeholder := NewEvent("publish event placeholder")
	e.eventManager.EmitEvent(placeholder)
}

func (e *EventPlaceholderManager) EmitEvents(events PublishEvents) {
	e.publishEventManager.EmitEvents(events)
	placeholders := make(Events, 0, len(events))
	for _, _ = range events {
		placeholder := NewEvent("publish event placeholder")
		placeholders = append(placeholders, placeholder)
	}
	e.eventManager.EmitEvents(placeholders)
}
