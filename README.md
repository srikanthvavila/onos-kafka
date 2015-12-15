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
For links 'mastership' is determined by the source device of the link.

## Configuration
