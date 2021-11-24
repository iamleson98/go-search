package message

type Message interface {
	Type() string // Type returns the type of this Message
}

type Queue interface {
	Close() error              // cleany shutdown the queue
	Enqueue(msg Message) error // Enqueue inserts a message to the end of the queue
	PendingMessages() bool     // PendingMessages returns true if the queue contains any messages
	DiscardMessages() error    // Flush drops all pending messages from the queue
	Messages() Iterator        // Messages returns an iterator for accessing the queued messages
}

type Iterator interface {
	Next() bool
	Message() Message
	Error() error
}

type QueueFactory func() Queue
