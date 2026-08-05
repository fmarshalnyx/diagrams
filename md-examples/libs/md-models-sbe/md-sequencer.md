module containing stand alone spring boot service.

purpose:
1. accept messages from input source.  All messages are defined in the md-models-sbe message set and will be sbe based.
    1.1. Ingress channels should be a bean
    1.2. initial implementation should be for a NatsSequencerIngress
        1.2.1.  create application.yml entry to define nats ingress:  server to connect to, and stream to subscribe to
2. assign a monotonic sequenceId to every received message.  this id is global across all received messages and will uniquely identify a message and the order the message was received
3. publish the message to a destination.
   3.1 destination channel should be pluggable.
   3.2 initial implementation should be for a nats destination
    3.2.1 create application.yml entry to define nats destination:  server to connect to, and stream to publish to

sequencer should  be designed to be run in kubernetes environment.  It is for a market data system that needs to minimize work and maximize throughput.

