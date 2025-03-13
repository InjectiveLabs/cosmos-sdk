package types

type PublishEventManagerI interface {
	Events() PublishEvents
	EmitEvent(event PublishEvent)
	EmitEvents(events PublishEvents)
}

var _ PublishEventManagerI = (*PublishEventManager)(nil)

// PublishEventManager implements a simple wrapper around a slice of PublishEvent objects that
// can be emitted from.
type PublishEventManager struct {
	events PublishEvents
}

func NewPublishEventManager() *PublishEventManager {
	return &PublishEventManager{EmptyPublishEvents()}
}

func (em *PublishEventManager) Events() PublishEvents { return em.events }

func (em *PublishEventManager) EmitEvent(event PublishEvent) {
	em.events = append(em.events, event)
}

func (em *PublishEventManager) EmitEvents(events PublishEvents) {
	em.events = append(em.events, events...)
}

type PublishEvent interface {
	Serialize() []byte
}

type PublishEvents []PublishEvent

// EmptyPublishEvents returns an empty slice of events.
func EmptyPublishEvents() PublishEvents {
	return make(PublishEvents, 0)
}
