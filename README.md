![](https://img.shields.io/badge/BluePlanet%20ONOS-1.3.1-blue.svg)
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
The Kafka instance to which the applciation binds defaults to `localhost:9092`.
This can be configured via the ONOS command line using the `cfg get` and
`cfg set` commands on the property
`org.ciena.onos.KafkaNotificationBridge kafka-server` the value when setting
the property should be `host:port`. 

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