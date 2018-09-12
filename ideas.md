setup a commander group that contains the event and command topic.
These groups need to be set on initialization for commander to work properly.

---

Consumer go routines etc.

Rethink creation of commands and events

on handle pass ctx (group) to the callback function

---

use a config struct to construct groups consumers and producers

```
config := commander.NewConfig()
config.Brokers = []string{"..."}

config.NewCluster(sarama.V1_0_0_0)
config.NewProducer()
config.NewConsumer()

cmdr := commander.New(&config)

cart := commander.Group{
  Name: "cart",
  Events: "cart-events",
  Commands: "cart-commands"
}

cmdr.RegisterGroup(cart)
go cmdr.StartConsuming()

cart.OnCommandHandle("AddItem", func(command) *commander.Event {
  command := ...

  event, err := warehouse.SyncCommand(command)
  if err != nil {
    panic(err)
  }

  return event
})

... event
```
