setup a commander group that contains the event and command topic.
These groups need to be set on initialization for commander to work properly.

---

Consumer go routines etc.

Rethink creation of commands and events

on handle pass ctx (group) to the callback function

---

use a config struct to construct groups consumers and producers

```
cart := commander.Group{
  Name: "cart",
  EventsTopic: commander.Topic{
    Name: "cart-events",
  },
  CommandsTopic: commander.Topic{
    Name: "cart-commands",
  }
}

warehouse := commander.Group{
  Name: "warehouse",
  EventsTopic: commander.Topic{
    Name: "warehouse-events",
  },
  CommandsTopic: commander.Topic{
    Name: "warehouse-commands",
  }
}

config := commander.NewConfig()
config.Brokers = []string{"..."}

config.NewCluster(sarama.V1_0_0_0)
config.AddGroups(&cart, &warehouse)

cmdr := commander.New(&config)
go cmdr.Consume()

cart.OnCommandHandle("AddItem", func(command) *commander.Event {
  command := ...

  event, err := warehouse.SyncCommand(command)
  if err != nil {
    panic(err)
  }

  return event
})
```
