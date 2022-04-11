# Cancel Range Scan (0xDC)

Requests that the server cancels an existing range scan.

The command uses an extras section to describe the input parameters.

## Extra definition

The extras for a continue range scan encodes:

* 128-bit uuid identifying the scan

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
       Total 16bytes
```

### Errors

Additional to common errors such as validation failure, auth-failure and
not-my-vbucket the following errors can occur.

**PROTOCOL_BINARY_RESPONSE_KEY_NO_EXISTS (0x01)**

The scan identifier has no match.
