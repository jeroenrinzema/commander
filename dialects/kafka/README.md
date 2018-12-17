# Kafka dialect

This dialect enables to setup a connection with Commander to Apache Kafka.

## Connection string

A connection string is required to connect to the Kafka cluster.
The string is build out of key's and value's in a linux like flag syntax: "-`key`=`value`".
An connection string could consist out of the following flags:

- **brokers**: should contain the ip addresses of the brokers in the Kafka cluster seperated by a `,`
- **group**: the Kafka consumer used to consume messages, the consumer group offset stored in kafka is used
- **version**: the Kafka version of the cluster

**Example:**

```
brokers=192.168.2.1,192.168.2.2 group=example version=1.1.0
```
