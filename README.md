![](https://img.shields.io/badge/BluePlanet%20ONOS-1.3.1%20%7C%201.4-blue.svg)
![](https://img.shields.io/badge/Apache%20Kafka-0.8.2.1-red.svg)
![](https://img.shields.io/badge/ONOS-Supports%20Clustering-brightgreen.svg)
# Kafka Based Device and Link Notification Publisher for ONOS

This project provides an ONOS platform application that once installed an
activated in ONOS will publish notifications from the `DeviceService` and
`LinkService` to the Kafka message system. 

## Overview
This application adds a listener on the `DeviceService` and `LinkService` in
ONOS and then when the listener is called will publish that event in JSON
format on the configured Kafka message bus.

## Clustering
When operating in a clustered deployment each application instance will only
publish notifications for those devices and links for which it is the master.
For links 'mastership' is determined by the destination device of the link.

## Configuration
The Kafka configuraiton parameters have been exposed as ONOS options and can be
queried and modified via the ONOS command line using the
`cfg get org.ciena.onos.onos-kafka` and `cfg set org.ciena.onos.onos-kafka` 
commands on the properties. The available options and there default values are:


| Name | Default | Description |
|---|---|---|
|kafka.acks|1|The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent|
|kafka.batch.size|16384|The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes|
|kafka.block.on.buffer.full|true|When our memory buffer is exhausted we must either stop accepting new records (block) or throw errors.|
|kafka.bootstrap.servers|localhost:9092|A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers|
|kafka.buffer.memory|33554432|The total bytes of memory the producer can use to buffer records waiting to be sent to the server|
|kafka.client.id|ONOS Node ID|An id string to pass to the server when making requests.|
|kafka.compression.type|none|Specify the final compression type for a given topic.|
|kafka.linger.ms|1|The producer groups together any records that arrive in between request transmissions into a single batched request.|
|kafka.max.in.flight.requests.per.connection|5|The maximum number of unacknowledged requests the client will send on a single connection before blocking.|
|kafka.max.request.size|1048576|The maximum size of a request. This is also effectively a cap on the maximum record size.|
|kafka.metadata.fetch.timeout|60000|The first time data is sent to a topic we must fetch metadata about that topic to know which servers host the topic's partitions. This fetch to succeed before throwing an exception back to the client.|
|kafka.metadata.max.age|300000|he period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.|
|kafka.metrics.num.samples|2|The number of samples maintained to compute metrics.|
|kafka.metrics.sample.window.ms|30000|The number of samples maintained to compute metrics.|
|kafka.receive.buffer|32768|The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.|
|kafka.reconnect.backoff.ms|10|The amount of time to wait before attempting to reconnect to a given host.|
|kafka.retries|0|Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.|
|kafka.retry.backoff.ms|100|The amount of time to wait before attempting to retry a failed fetch request to a given topic partition.|
|kafka.send.buffer|131072|The size of the TCP send buffer (SO_SNDBUF) to use when sending data.|
|kafka.timeout|30000|The configuration controls the maximum amount of time the client will wait for the response of a request.|

## JSON Encoding
The Kafka bridge application encodes the `DeviceEvent` and the `LinkEvent` as
depicted in the following examples.

#### DeviceEvent
    // Without Port Information
    {  
       "type":"DEVICE_UPDATED",
       "time":1450303251479,
       "subject":{  
          "id":"of:0000000000000001",
          "chassis":"1",
          "hw-version":"Open vSwitch",
          "manufacturer":"Nicira, Inc.",
          "serial-number":"None",
          "sw-version":"2.3.2",
          "provider":"org.onosproject.provider.openflow",
          "type":"SWITCH"
       }
    }
    
    // With Port Information
    {
       "type":"PORT_UPDATED",
       "time":1450304732512,
       "subject":{
          "id":"of:0000000000000001",
          "chassis":"1",
          "hw-version":"Open vSwitch",
          "manufacturer":"Nicira, Inc.",
          "serial-number":"None",
          "sw-version":"2.3.2",
          "provider":"org.onosproject.provider.openflow",
          "type":"SWITCH"
       },
       "port":{
          "type":"COPPER",
          "number":2,
          "speed":10,
          "enabled":true
       }
    }

#### LinkEvent
    {  
       "type":"LINK_REMOVED",
       "time":1450303125469,
       "src":"of:0000000000000003",
       "src-port":3,
       "dst":"of:0000000000000002",
       "dst-port":1,
       "state":"ACTIVE",
       "durable":false
    }