# Create Range Scan (0xDA)

Requests that the server creates a new range scan against a vbucket.

The request:
* Must have extras
* No key
* A value (which is the start and end key together)

A number of variations of input parameters are possible, each uses a different 
extras section to describe the input. Each extras section begins with a 32-bit
version/ flags field. The version data allows the client/server to distinguish
which extras structure is being described.

## Extra data for basic range scan

The extras for a basic range scan encodes:

* version 0 and any flags
* the collection ID to scan
* the lengths of start/end key (which will follow the header data)


```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| version and flags                                             |
       +---------------+---------------+---------------+---------------+
      4| collection ID                                                 |
       +---------------+---------------+---------------+---------------+
      8| start key len | end key len   |
       +---------------+---------------+
       Total 10bytes
```

## Extra data for range scan with sequence number/uuid

The extras for a range scan with sequence number/uuid encodes:

* version 1 and any flags
* the collection ID to scan
* millisecond timeout, the server will wait this long for the sequence number to be persisted (0 no wait)
* vbucket uuid, command can only succeed if the vbucket has a matching uuid
* sequence number, command can only succeed if this sequence number has been persisted
* the lengths of start/end key (which will follow the header data)

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| version and flags                                             |
       +---------------+---------------+---------------+---------------+
      4| collection ID                                                 |
       +---------------+---------------+---------------+---------------+
      8| timeout (ms)                                                  |
       +---------------+---------------+---------------+---------------+
     12| vbucket uuid                                                  |
       +                                                               +
     16|                                                               |
       +---------------+---------------+---------------+---------------+
     20| sequence number                                               |
       +                                                               +
     24|                                                               |
       +---------------+---------------+---------------+---------------+
     28| start key len | end key len   |
       +---------------+---------------+
       Total 30bytes
```

## Extra data for sampling scan

The extras for a sampling scan encodes:

* version 2 and any flags
* the collection ID to scan
* random seed
* samples to return

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| version and flags                                             |
       +---------------+---------------+---------------+---------------+
      4| collection ID                                                 |
       +---------------+---------------+---------------+---------------+
      8| random seed                                                   |
       +---------------+---------------+---------------+---------------+
     12| samples                                                       |
       +---------------+---------------+---------------+---------------+
       Total 16bytes

```

## Extra data for sampling scan (with seqno/vb-uuid)

The extras for a sampling scan encodes:

* version 3 and any flags
* the collection ID to scan
* random seed
* samples to return

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| version and flags                                             |
       +---------------+---------------+---------------+---------------+
      4| collection ID                                                 |
       +---------------+---------------+---------------+---------------+
      8| random seed                                                   |
       +---------------+---------------+---------------+---------------+
     12| samples                                                       |
       +---------------+---------------+---------------+---------------+
     16| timeout (ms)                                                  |
       +---------------+---------------+---------------+---------------+
     20| vbucket uuid                                                  |
       +                                                               +
     24|                                                               |
       +---------------+---------------+---------------+---------------+
     28| sequence number                                               |
       +                                                               +
     32|                                                               |
       +---------------+---------------+---------------+---------------+
       Total 36bytes

```

## version and flags

A 32-bit field encodes both the version and configuration flags.

The least significant byte encodes a 1 byte version field and then following
flags are defined.

* flag0: clear = scan returns documents (key/meta/value), set = scan return keys
* flag1: clear = start key is inclusive, set = start key is exclusive
* flag2: clear = end key is inclusive, set = end key is exclusive
* flag3: set = sequence number must still exist in snapshot

flag1/flag2 are ignored for sampling scan
flag3 is ignored for scans without vb-uuid/seqno

# Example

Example shows a request to scan:

* vbucket 528
* collection 1000
* From key "2010" to "2022"
* key-only (flag0 set)
* Require vb-uuid and sequence number
** uuid is 0x1122.3344.5566.7788
** seqno is 4174498
** timeout of 5 seconds

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0xDA          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x1E          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x26          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x01          | 0x01          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x03          | 0xE8          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x13          | 0x88          |
        +---------------+---------------+---------------+---------------+
      36| 0x11          | 0x22          | 0x33          | 0x44          |
        +---------------+---------------+---------------+---------------+
      40| 0x55          | 0x66          | 0x77          | 0x88          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x3F          | 0xB2          | 0xA2          |
        +---------------+---------------+---------------+---------------+
      52| 0x04          | 0x04          | 0x32 ('2')    | 0x30 ('0')    |
        +---------------+---------------+---------------+---------------+
      56| 0x31 ('1')    | 0x30 ('0')    | 0x32 ('2')    | 0x30 ('0')    |
        +---------------+---------------+---------------+---------------+
      60| 0x32 ('2')    | 0x32 ('2')    |
        +---------------+---------------+
      Total 62 bytes (24 byte header + 30 byte extras + 8 byte value)

         CREATE_RANGE_SCAN command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0xDA
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x1E
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x00000026
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
    extras...
     vers/flags  (24-27): 0x00000101
     collection  (28-31): 0x000003e8
     timeout     (32-35): 0x00001388
     vb-uuid     (36-43): 0x1122334455667788
     seqno       (44-51): 0x00000000003FB2A2
     start len   (52):    0x04
     end len     (53):    0x04
    value...
     start       (54-57): "2010"
     end         (58-61): "2022"

### Returns

On success the response will include a 128-bit (16-byte) identifier that the
client should use in continue and cancel requests. This will be encoded as
a value of the response packet.

         CREATE_RANGE_SCAN success
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0xDA
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000010
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
    uuid         (24-39): .... @todo clarify how this is byte ordered

### Errors

Additional to common errors such as validation failure, auth-failure and
not-my-vbucket the following errors can occur.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

For this command this response can occur if the optional vb-uuid does not match
the vbucket.

**PROTOCOL_BINARY_RESPONSE_TEMPORARY_FAILURE (0x86)**

The requested sequence number was not available (with or without a timeout).

