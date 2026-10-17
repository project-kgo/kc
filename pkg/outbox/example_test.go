package outbox_test

import (
	"fmt"

	"github.com/project-kgo/kc/pkg/outbox"
)

func ExampleNewMessage() {
	message, err := outbox.NewMessage("order.created", struct {
		OrderID int64 `json:"order_id"`
	}{OrderID: 9223372036854775807})
	if err != nil {
		panic(err)
	}
	var event struct {
		OrderID int64 `json:"order_id"`
	}
	if err := message.Decode(&event); err != nil {
		panic(err)
	}
	fmt.Println(message.Type, event.OrderID)
	// Output: order.created 9223372036854775807
}
