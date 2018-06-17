# Commander

Commander is a pattern for writing distributed applications using ideas from CQRS and Event Sourcing using Kafka as transportation layer.

> 🚧 This project is currently under active development

## Overview
- **High performing** - Commander uses Kafka a distributed, fault-tolerant and wicket fast streaming platform as it's transportation layer
- **Encryption** - All stored events can be easily encrypted and decrypted
- **Developer friendly** - We aim to create developer friendly APIs that get you started quickly

## Getting started

To get started quickly download/clone the [boilerplate project](https://github.com/sysco-middleware/commander-boilerplate).

Every group has a command, query, materialise and logic service. The command and query services can easily be included in your project via docker images. But if required can you fork the services and modify them to your needs. Every group is responsible for a single dataset.

## GDPR

Commander offers various APIs to handle GDPR complaints. To keep the immutable ledger, immutable do we offer the plausibility to encrypt all events. Once a "right to erasure" request needs to be preformed can all data be erased by simply throwing away the key.
