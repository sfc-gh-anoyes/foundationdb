# Flow Transport

This section describes the design and implementation of the flow transport wire protocol (as of release 6.3).

## ConnectPacket

The first bytes sent over a tcp connection in flow are the `ConnectPacket`.
This is a variable length message (though fixed length at a given protocol
version) designed with forward and backward compatibility in mind. The expected length of the `ConnectPacket` is encoded as the first 4 bytes (unsigned, little-endian). Upon receiving an incoming connection, a peer reads the `ProtocolVersion` (the next 8 bytes unsigned, little-endian. The most significant 4 bits encode flags and should be zeroed before interpreting numerically.) from the `ConnectPacket`.

## Protocol compatibility

Based on the incoming connection's `ProtocolVersion`, this connection is either
"compatible" or "incompatible". If this connection is incompatible, then we
will not actually look at any bytes sent after the `ConnectPacket`, but we will
keep the connection open so that the peer does not keep trying to open new
connections.

If this connection is compatible, then we know that our peer is using the same wire protocol as we are and we can proceed.

## Framing and checksumming protocol

As of release 6.3, the structure of subsequent messages is as follows:

* For TLS connections:
    1. packet length (4 bytes unsigned little-endian)
    2. token (16 opaque bytes that identify the recipient of this message)
    3. message contents (packet length - 16 bytes to be interpreted by the recipient)
* For non-TLS connections, there's additionally a crc32 checksum for message integrity:
    1. packet length (4 bytes unsigned little-endian)
    2. 4 byte crc32 checksum of token + message
    3. token
    4. message

## Well-known endpoints

Endpoints are a pair of a 16 byte token that identifies the recipient and a
network address to send a message to. Endpoints are usually obtained over the
network - for example a request conventionally includes the endpoint the
reply should be sent to (like a self-addressed stamped envelope). So if you
can send a message and get endpoints in reply you can start sending messages
those endpoints. But how do you send that first message?

That's where the concept of a "well-known" endpoint comes in. Some endpoints
(for example the endpoints coordinators are listening on) use "well-known"
tokens that are agreed upon ahead of time. Technically the value of these
tokens could be changed as part of an incompatible protocol version bump, but
in practice this hasn't happened and shouldn't ever need to happen.

## Flatbuffers

Prior to release-6.2 the structure of messages (e.g. how many fields a
message has) was implicitly part of the protocol version, and so adding a
field to any message required a protocol version bump. Since release-6.2
messages are encoded as flatbuffers messages, and you can technically add
fields without a protocol version bump. This is a powerful and dangerous tool
that needs to be used with caution. If you add a field without a protocol version bump, then you can no longer be certain that this field will always be present (e.g. if you get a message from an old peer it might not include that field.) 
We don't have a good way to test two or more fdbserver binaries in
simulation, so we discourage adding fields or otherwise making any protocol
changes without a protocol version bump.

Bumping the protocol version is costly for clients though, since now they need a whole new libfdb_c.so to be able to talk to the cluster _at all_.

## Stable Endpoints

Stable endpoints are a proposal to allow protocol compatibility to be checked
per endpoint rather than per connection. The proposal is to commit to a
framing protocol designed for forward and backward compatibility (with any
release since release-7.0). This way even if peers are at different protocol
versions they can still read the token each message is addressed to, and they
can use that token to decide whether or not to attempt to handle the message.
By default, tokens will have the same compatibility requirements as before
where the protocol version must match exactly. But new tokens can optionally
have a different policy - e.g. handle anything from a protocol version >=
release-7.0.

One of the main features motivating "Stable Endpoints" is the ability to ask an incompatible fdbserver what it's protocol version is from the client.

### Changes to flow transport required for Stable Endpoints

1. Well known endpoints must never change (this just makes it official)
2. The basic framing protocol
