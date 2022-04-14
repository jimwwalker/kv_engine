# Continue Range Scan (0xDB)

Requests that the server continues an existing range scan, returning to the 
client a sequence of keys or documents (key/meta/value e.g. like GetWithMeta).

A continue will return at least 1 key/document unless:

* scan completes
* an error occurs

The client is obligated to keep continuing a scan in a timely fashion. The
server will cancel scans that are idle for too long or not making enough
progress.

The command uses an extras section to describe the input parameters.

## Extra definition

The extras for a continue range scan encodes:

* 128-bit uuid identifying the scan
* maximum item count to return (0 no limit)
* maximum time (ms) limit for the scan to run for (0 no limit)

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
     16| item limit                                                    |
       +---------------+---------------+---------------+---------------+
     20| time limit                                                    |
       +---------------+---------------+---------------+---------------+
       Total 24bytes
```

### Errors

Additional to common errors such as validation failure, auth-failure and
not-my-vbucket the following errors can occur.

**PROTOCOL_BINARY_RESPONSE_KEY_NO_EXISTS (0x01)**

The scan identifier has no match.
