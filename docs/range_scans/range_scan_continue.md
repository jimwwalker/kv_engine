# Range Scan Continue (0xDB)

Requests that the server continues an existing range scan, returning to the 
client a sequence of keys or documents. The client can limit how many documents
are returned and/or how much time the continue request can run for.

The request:
* Must contain extras
* No key
* No value

A continue will return at least 1 key/document unless:

* scan reaches the end
* an error occurs

The client is obligated to keep continuing a scan. The server will cancel scans
that are idle for too long or not making enough progress (TBD, scans can
currently remain indefinitely idle).


The command uses an extras section to describe the input parameters.

## Extra definition

The extras for a continue range scan encodes:

* 128-bit uuid identifying the scan (obtained from range-scan-create)
* Maximum key/document count to return (when 0 there is no limit)
* Maximum time (ms) for the scan to keep returning key/documents (when 0 there
  is no time limit)

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| scan identifier                                               |
       +                                                               +
      4|                                                               |
       +                                                               +
      8|                                                               |
       +                                                               +
     12|                                                               |
       +                                                               +
       +---------------+---------------+---------------+---------------+
     16| item limit (network byte order)                               |
       +---------------+---------------+---------------+---------------+
     20| time limit (network byte order)                               |
       +---------------+---------------+---------------+---------------+
       Total 24bytes
```

## Response format

A successful continue will return key/documents using the following framing. A
series of key/documents always begins with the standard mcbp 24-byte response
header which will include a success status and a value, the value encodes many
keys/documents.

Multiple sequences of key/documents maybe returned until the continue informs
the client the sequence has ended. The end of the sequence is a the standard
mcbp 24-byte response with a status code that is not success. The status could
be an error or just informational regarding the state of the scan.

For example a range covers 5,000 keys, the client only wants to handle 500 keys
per continue. Thus the client would first issue a range-scan-continue (0xDB) with
the uuid from range-scan-create (0xDA), item limit set to 500 and the time limit
set to 0.

The server will respond with a series of responses, there is no definition of
how many keys each response may encode.

* client request item-limit=500
* response status=success, bodylen=2612
* response status=success, bodylen=2595
* response status=success, bodylen=2602
* response status=range-scan-more (0xa6), bodylen=0  <- sequence end

In this sequence 500 keys are returned (distributed over 4 different responses).
The final response has the status of range-scan-more informing the client that
the scan has more data.

The client processes the returned data and then issues more range-scan-continue
requests. Finally the client is informed that all data has been returned as
a mcbp response with success and bodylen 0 is returned e.gg.

* client request item-limit=500
* response status=success, bodylen=2626
* response status=success, bodylen=0 <- sequence end and scan complete

### Response Value Encoding `"key_only":true`

The mcbp header defines how much data follows, in this command only a value is
transmitted (key and extras fields are both 0). The format of the value is a
sequence of length/key pairs. Each length is a varint (leb128) encoding of the
key length.

For example if 3 keys are returned in one response, "key0", "key11", "key222",
the value encodes the following sequence.

* length=4, "key0"
* length=5, "key11"
* length=6, "key222"


```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| 0x04          | 'k' 0x6B      | 'e' 0x65      | 'y' 0x79      |
       +---------------+---------------+---------------+---------------+
      4| '0' 0x30      | 0x05          | 'k' 0x6B      | 'e' 0x65      |
       +---------------+---------------+---------------+---------------+
      8| 'y' 0x79      | '1' 0x31      | '1' 0x31      | 0x6           |
       +---------------+---------------+---------------+---------------+
     12| 'k' 0x6B      | 'e' 0x65      | 'y' 0x79      | '2' 0x32      |
       +---------------+---------------+---------------+---------------+
     16| '2' 0x32      | '2' 0x32      |
       +---------------+---------------+
       Total 17bytes
```

### Response Value Encoding `"key_only":false`

The mcbp header defines how much data follows, in this command only a value is
transmitted (key and extras fields are both 0). The format of the value is a
sequence of documents. Each document has the following 3 parts.

* Fixed length metadata
* variable length key
* variable length value

The document encoding will prefix the variable length key and value each with
a varint (leb128).

For example if 2 documents are returned in one response.

* document1 {fixed meta}{varint, key}{varint, value}
* document2 {fixed meta}{varint, key}{varint, value}

The format of a single document is now described in more detail.

Fixed metadata (25 bytes)
* 32-bit flags
* 32-bit expiry
* 64-bit seqno
* 64-bit cas
* 8-bit datatype - Note the datatype is critical to interpreting the value,
  e.g. if datatype has the snappy bit set, the value is compressed.

E.g. a document with key="key0" and value="value0"

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| flags (network order)                                         |
       +---------------+---------------+---------------+---------------+
      4| expiry (network order)                                        |
       +---------------+---------------+---------------+---------------+
      8| sequence number  (network order)                              |
       +                                                               +
     12|                                                               |
       +---------------+---------------+---------------+---------------+
     16| cas (network order)                                           |
       +                                                               +
     20|                                                               |
       +---------------+---------------+---------------+---------------+
     24| datatype      | 0x04          | 'k' 0x6B      | 'e' 0x65      |
       +---------------+---------------+---------------+---------------+
     28| 'y' 0x79      | '0' 0x30      | 0x06          | 'v' 0xaa      |
       +---------------+---------------+---------------+---------------+
     32| 'a' 0x79      | 'l' 0x30      | 'u' 0xaa      | 'e' 0xaa      |
       +---------------+---------------+---------------+---------------+
     36| '0' 0x30      | <next document>
       +---------------+---------------...
       Total 36bytes
```

In the above layout, a second document would begin at offset 37, no padding or
alignment.

### Errors

Additional to common errors such as validation failure, auth-failure and
not-my-vbucket the following errors can occur.

**PROTOCOL_BINARY_RESPONSE_KEY_NO_EXISTS (0x01)**

No scan with the given uuid could be found.

**PROTOCOL_BINARY_RESPONSE_RANGE_SCAN_CANCELLED (0xA5)**

The scan was cancelled whilst returning data. This could be the only status if
the cancel was noticed before a key/value was loaded.
