# Kafka dialect

This dialect enables to setup a connection with Commander to Apache Kafka.

## Connection string

A connection string is required to connect to the Kafka cluster.
The string is build out of key's and value's in a linux like flag syntax: "-`key`=`value`".
An connection string could consist out of the following flags:

- **brokers**: should contain the ip addresses of the brokers in the Kafka cluster seperated by a `,`
- **group** *optional*: the Kafka consumer group used to consume messages, when defined is a new consumer group set-up and is the latest marked offset stored. When no group is defined/given is a partition consumer created.
- **version**: the Kafka version of the cluster
- **initial-offset**: the initial offset used when setting up a partition consumer. The initial offset could be one of the following values: (int)0.../"newest"/"oldest"

**Example:**

```
brokers=192.168.2.1,192.168.2.2 group=example version=2.1.1
```

```
brokers=192.168.2.1,192.168.2.2 initial-offset=oldest version=2.1.1
```