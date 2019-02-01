/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once
#include "client_connection.h"
#include <boost/optional/optional_fwd.hpp>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/response.h>
#include <nlohmann/json.hpp>
#include <platform/sized_buffer.h>
#include <unordered_set>
/**
 * This is the base class used for binary protocol commands. You probably
 * want to use one of the subclasses. Do not subclass this class directly,
 * rather, instantiate/derive from BinprotCommandT or BinprotGenericCommand
 */
class BinprotCommand {
public:
    BinprotCommand(const BinprotCommand&) = delete;

    BinprotCommand() = default;

    virtual ~BinprotCommand() = default;

    cb::mcbp::ClientOpcode getOp() const;

    const std::string& getKey() const;

    uint64_t getCas() const;

    virtual void clear();

    BinprotCommand& setKey(std::string key_);

    BinprotCommand& setCas(uint64_t cas_);

    BinprotCommand& setOp(cb::mcbp::ClientOpcode cmd_);

    BinprotCommand& setVBucket(Vbid vbid);

    /**
     * Encode the command to a buffer.
     * @param buf The buffer
     * @note the buffer's contents are _not_ reset, and the encoded command
     *       is simply appended to it.
     *
     * The default implementation is to encode the standard header fields.
     * The key itself is not added to the buffer.
     */
    virtual void encode(std::vector<uint8_t>& buf) const;

    struct Encoded {
        Encoded();

        /**
         * 'scratch' space for data which isn't owned by anything and is
         * generated on demand. Any data here is sent before the data in the
         * buffers.
         */
        std::vector<uint8_t> header;

        /** The actual buffers to be sent */
        std::vector<cb::const_byte_buffer> bufs;
    };

    /**
     * Encode data into an 'Encoded' object.
     * @return an `Encoded` object which may be sent on the wire.
     *
     * Note that unlike the vector<uint8_t> variant, the actual buffers
     * are not copied into the new structure, so ensure the command object
     * (which owns the buffers), or the original buffers (if the command object
     * doesn't own the buffers either; see e.g.
     * BinprotMutationCommand::setValueBuffers()) remain in tact between this
     * call and actually sending it.
     *
     * The default implementation simply copies what encode(vector<uint8_t>)
     * does into Encoded::header, and Encoded::bufs contains a single
     * element.
     */
    virtual Encoded encode() const;
protected:
    /**
     * This class exposes a tri-state expiry object, to allow for a 0-value
     * expiry. This is not used directly by this class, but is used a bit in
     * subclasses
     */
    class ExpiryValue {
    public:
        void assign(uint32_t value_);
        void clear();
        bool isSet() const;
        uint32_t getValue() const;

    private:
        bool set = false;
        uint32_t value = 0;
    };

    /**
     * Fills the header with the current fields.
     *
     * @param[out] header header to write to
     * @param payload_len length of the "value" of the payload
     * @param extlen extras length.
     */
    void fillHeader(cb::mcbp::Request& header,
                    size_t payload_len = 0,
                    size_t extlen = 0) const;

    /**
     * Writes the header to the buffer
     * @param buf Buffer to write to
     * @param payload_len Payload length (excluding keylen and extlen)
     * @param extlen Length of extras
     */
    void writeHeader(std::vector<uint8_t>& buf,
                     size_t payload_len = 0,
                     size_t extlen = 0) const;

    cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::Invalid;
    std::string key;
    uint64_t cas = 0;
    Vbid vbucket = Vbid(0);
};

/**
 * For use with subclasses of @class MemcachedCommand. This installs setter
 * methods which
 * return the actual class rather than TestCmd.
 *
 * @code{.c++}
 * class SomeCmd : public MemcachedCommandT<SomeCmd> {
 *   // ...
 * };
 * @endcode
 *
 * And therefore allows subclasses to be used like so:
 *
 * @code{.c++}
 * MyCommand cmd;
 * cmd.setKey("foo").
 *     setExpiry(300).
 *     setCas(0xdeadbeef);
 *
 * @endcode
 */
template <typename T,
          cb::mcbp::ClientOpcode OpCode = cb::mcbp::ClientOpcode::Invalid>
class BinprotCommandT : public BinprotCommand {
public:
    BinprotCommandT() {
        setOp(OpCode);
    }
};

/**
 * Convenience class for constructing ad-hoc commands with no special semantics.
 * Ideally, you should use another class which provides nicer wrapper functions.
 */
class BinprotGenericCommand : public BinprotCommandT<BinprotGenericCommand> {
public:
    BinprotGenericCommand(cb::mcbp::ClientOpcode opcode,
                          const std::string& key_,
                          const std::string& value_);
    BinprotGenericCommand(cb::mcbp::ClientOpcode opcode,
                          const std::string& key_);
    BinprotGenericCommand(cb::mcbp::ClientOpcode opcode);
    BinprotGenericCommand();
    BinprotGenericCommand& setValue(std::string value_);
    BinprotGenericCommand& setExtras(const std::vector<uint8_t>& buf);

    // Use for setting a simple value as an extras
    template <typename T>
    BinprotGenericCommand& setExtrasValue(T value) {
        std::vector<uint8_t> buf;
        buf.resize(sizeof(T));
        memcpy(buf.data(), &value, sizeof(T));
        return setExtras(buf);
    }

    void clear() override;

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    std::string value;
    std::vector<uint8_t> extras;
};

class BinprotResponse {
public:
    bool isSuccess() const;

    /** Get the opcode for the response */
    cb::mcbp::ClientOpcode getOp() const;

    /** Get the status code for the response */
    cb::mcbp::Status getStatus() const;

    size_t getExtlen() const;

    /** Get the length of packet (minus the header) */
    size_t getBodylen() const;

    size_t getFramingExtraslen() const;

    /**
     * Get the length of the header. This is a static function as it is
     * always 24
     */
    static size_t getHeaderLen();
    uint64_t getCas() const;
    protocol_binary_datatype_t getDatatype() const;

    /**
     * Get a pointer to the payload of the response. This begins immediately
     * after the 24 byte memcached header
     */
    const uint8_t* getPayload() const;

    /**
     * Get a pointer to the key returned in the packet, if a key is present.
     * Use #getKeyLen() to determine this.
     */
    cb::const_char_buffer getKey() const;

    std::string getKeyString() const;

    /**
     * Get a pointer to the "data" or "value" part of the response. This is
     * any payload content _after_ the key and extras (if present
     */
    cb::const_byte_buffer getData() const;

    std::string getDataString() const;

    const cb::mcbp::Response& getResponse() const;

    /**
     * Retrieve the approximate time spent on the server
     */
    boost::optional<std::chrono::microseconds> getTracingData() const;

    /**
     * Populate this response from a response
     * @param srcbuf The buffer containing the response.
     *
     * The input parameter here is forced to be an rvalue reference because
     * we don't want careless copying of potentially large payloads.
     */
    virtual void assign(std::vector<uint8_t>&& srcbuf);

    virtual void clear();

    virtual ~BinprotResponse() = default;

protected:
    const cb::mcbp::Header& getHeader() const;

    const uint8_t* begin() const;

    std::vector<uint8_t> payload;
};

class BinprotSubdocCommand : public BinprotCommandT<BinprotSubdocCommand> {
public:
    BinprotSubdocCommand();

    explicit BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_);

    // Old-style constructors. These are all used by testapp_subdoc.
    BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_,
                         const std::string& key_,
                         const std::string& path_);

    BinprotSubdocCommand(
            cb::mcbp::ClientOpcode cmd,
            const std::string& key,
            const std::string& path,
            const std::string& value,
            protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag docFlags = mcbp::subdoc::doc_flag::None,
            uint64_t cas = 0);

    BinprotSubdocCommand& setPath(std::string path_);
    BinprotSubdocCommand& setValue(std::string value_);
    BinprotSubdocCommand& addPathFlags(protocol_binary_subdoc_flag flags_);
    BinprotSubdocCommand& addDocFlags(mcbp::subdoc::doc_flag flags_);
    BinprotSubdocCommand& setExpiry(uint32_t value_);
    const std::string& getPath() const;
    const std::string& getValue() const;
    protocol_binary_subdoc_flag getFlags() const;

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::string path;
    std::string value;
    BinprotCommand::ExpiryValue expiry;
    protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE;
    mcbp::subdoc::doc_flag doc_flags = mcbp::subdoc::doc_flag::None;
};

class BinprotSubdocResponse : public BinprotResponse {
public:
    const std::string& getValue() const;

    void clear() override;

    void assign(std::vector<uint8_t>&& srcbuf) override;

    bool operator==(const BinprotSubdocResponse& other) const;

private:
    std::string value;
};

class BinprotSubdocMultiMutationCommand
    : public BinprotCommandT<BinprotSubdocMultiMutationCommand,
                             cb::mcbp::ClientOpcode::SubdocMultiMutation> {
public:
    BinprotSubdocMultiMutationCommand();

    struct MutationSpecifier {
        cb::mcbp::ClientOpcode opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
        std::string value;
    };

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotSubdocMultiMutationCommand& addDocFlag(
            mcbp::subdoc::doc_flag docFlag);

    BinprotSubdocMultiMutationCommand& addMutation(
            const MutationSpecifier& spec);

    BinprotSubdocMultiMutationCommand& addMutation(
            cb::mcbp::ClientOpcode opcode,
            protocol_binary_subdoc_flag flags,
            const std::string& path,
            const std::string& value);

    BinprotSubdocMultiMutationCommand& setExpiry(uint32_t expiry_);

    MutationSpecifier& at(size_t index);

    MutationSpecifier& operator[](size_t index);

    bool empty() const;

    size_t size() const;

    void clearMutations();

    void clearDocFlags();

protected:
    std::vector<MutationSpecifier> specs;
    ExpiryValue expiry;
    mcbp::subdoc::doc_flag docFlags;
};

class BinprotSubdocMultiMutationResponse : public BinprotResponse {
public:
    struct MutationResult {
        uint8_t index;
        cb::mcbp::Status status;
        std::string value;
    };

    void assign(std::vector<uint8_t>&& buf) override;

    void clear() override;

    const std::vector<MutationResult>& getResults() const;

protected:
    std::vector<MutationResult> results;
};

class BinprotSubdocMultiLookupCommand
    : public BinprotCommandT<BinprotSubdocMultiLookupCommand,
                             cb::mcbp::ClientOpcode::SubdocMultiLookup> {
public:
    BinprotSubdocMultiLookupCommand();

    struct LookupSpecifier {
        cb::mcbp::ClientOpcode opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
    };

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotSubdocMultiLookupCommand& addLookup(const LookupSpecifier& spec);

    BinprotSubdocMultiLookupCommand& addLookup(
            const std::string& path,
            cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::SubdocGet,
            protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE);

    BinprotSubdocMultiLookupCommand& addGet(
            const std::string& path,
            protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE);
    BinprotSubdocMultiLookupCommand& addExists(
            const std::string& path,
            protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE);
    BinprotSubdocMultiLookupCommand& addGetcount(
            const std::string& path,
            protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE);
    BinprotSubdocMultiLookupCommand& addDocFlag(mcbp::subdoc::doc_flag docFlag);

    void clearLookups();

    LookupSpecifier& at(size_t index);

    LookupSpecifier& operator[](size_t index);

    bool empty() const;

    size_t size() const;

    void clearDocFlags();

    /**
     * This is used for testing only!
     */
    BinprotSubdocMultiLookupCommand& setExpiry_Unsupported(uint32_t expiry_);

protected:
    std::vector<LookupSpecifier> specs;
    ExpiryValue expiry;
    mcbp::subdoc::doc_flag docFlags;
};

class BinprotSubdocMultiLookupResponse : public BinprotResponse {
public:
    struct LookupResult {
        cb::mcbp::Status status;
        std::string value;
    };

    const std::vector<LookupResult>& getResults() const;

    void clear() override;

    void assign(std::vector<uint8_t>&& srcbuf) override;

protected:
    std::vector<LookupResult> results;
};

class BinprotSaslAuthCommand
    : public BinprotCommandT<BinprotSaslAuthCommand,
                             cb::mcbp::ClientOpcode::SaslAuth> {
public:
    void setMechanism(const std::string& mech_);

    void setChallenge(cb::const_char_buffer data);

    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge;
};

class BinprotSaslStepCommand
    : public BinprotCommandT<BinprotSaslStepCommand,
                             cb::mcbp::ClientOpcode::SaslStep> {
public:
    void setMechanism(const std::string& mech);

    void setChallenge(cb::const_char_buffer data);

    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge;
};

class BinprotHelloCommand
    : public BinprotCommandT<BinprotHelloCommand,
                             cb::mcbp::ClientOpcode::Hello> {
public:
    explicit BinprotHelloCommand(const std::string& client_id);
    BinprotHelloCommand& enableFeature(cb::mcbp::Feature feature,
                                       bool enabled = true);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::unordered_set<uint16_t> features;
};

class BinprotHelloResponse : public BinprotResponse {
public:
    void assign(std::vector<uint8_t>&& buf) override;
    const std::vector<cb::mcbp::Feature>& getFeatures() const;

private:
    std::vector<cb::mcbp::Feature> features;
};

class BinprotCreateBucketCommand
    : public BinprotCommandT<BinprotCreateBucketCommand,
                             cb::mcbp::ClientOpcode::CreateBucket> {
public:
    explicit BinprotCreateBucketCommand(const char* name);

    void setConfig(const std::string& module, const std::string& config);
    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::vector<uint8_t> module_config;
};

class BinprotGetCommand
    : public BinprotCommandT<BinprotGetCommand, cb::mcbp::ClientOpcode::Get> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

class BinprotGetAndLockCommand
    : public BinprotCommandT<BinprotGetAndLockCommand,
                             cb::mcbp::ClientOpcode::GetLocked> {
public:
    BinprotGetAndLockCommand();

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotGetAndLockCommand& setLockTimeout(uint32_t timeout);

protected:
    uint32_t lock_timeout;
};

class BinprotGetAndTouchCommand
    : public BinprotCommandT<BinprotGetAndTouchCommand,
                             cb::mcbp::ClientOpcode::Gat> {
public:
    BinprotGetAndTouchCommand();

    void encode(std::vector<uint8_t>& buf) const override;

    bool isQuiet() const;

    BinprotGetAndTouchCommand& setQuiet(bool quiet = true);

    BinprotGetAndTouchCommand& setExpirytime(uint32_t timeout);

protected:
    uint32_t expirytime;
};

class BinprotGetResponse : public BinprotResponse {
public:
    uint32_t getDocumentFlags() const;
};

using BinprotGetAndLockResponse = BinprotGetResponse;
using BinprotGetAndTouchResponse = BinprotGetResponse;

class BinprotUnlockCommand
    : public BinprotCommandT<BinprotGetCommand,
                             cb::mcbp::ClientOpcode::UnlockKey> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

using BinprotUnlockResponse = BinprotResponse;

class BinprotTouchCommand
    : public BinprotCommandT<BinprotGetAndTouchCommand,
                             cb::mcbp::ClientOpcode::Touch> {
public:
    void encode(std::vector<uint8_t>& buf) const override;

    BinprotTouchCommand& setExpirytime(uint32_t timeout);

protected:
    uint32_t expirytime = 0;
};

using BinprotTouchResponse = BinprotResponse;

class BinprotGetCmdTimerCommand
    : public BinprotCommandT<BinprotGetCmdTimerCommand,
                             cb::mcbp::ClientOpcode::GetCmdTimer> {
public:
    BinprotGetCmdTimerCommand() = default;
    explicit BinprotGetCmdTimerCommand(cb::mcbp::ClientOpcode opcode);
    BinprotGetCmdTimerCommand(const std::string& bucket,
                              cb::mcbp::ClientOpcode opcode);

    void encode(std::vector<uint8_t>& buf) const override;

    void setOpcode(cb::mcbp::ClientOpcode opcode);

    void setBucket(const std::string& bucket);

protected:
    cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::Invalid;
};

class BinprotGetCmdTimerResponse : public BinprotResponse {
public:
    void assign(std::vector<uint8_t>&& buf) override;

    nlohmann::json getTimings() const;

private:
    nlohmann::json timings;
};

class BinprotVerbosityCommand
    : public BinprotCommandT<BinprotVerbosityCommand,
                             cb::mcbp::ClientOpcode::Verbosity> {
public:
    void encode(std::vector<uint8_t>& buf) const override;

    void setLevel(int level);

protected:
    int level;
};

using BinprotVerbosityResponse = BinprotResponse;

using BinprotIsaslRefreshCommand =
        BinprotCommandT<BinprotGenericCommand,
                        cb::mcbp::ClientOpcode::IsaslRefresh>;

using BinprotIsaslRefreshResponse = BinprotResponse;

class BinprotMutationCommand : public BinprotCommandT<BinprotMutationCommand> {
public:
    BinprotMutationCommand& setMutationType(MutationType);
    BinprotMutationCommand& setDocumentInfo(const DocumentInfo& info);

    BinprotMutationCommand& setValue(std::vector<uint8_t>&& value_);

    template <typename T>
    BinprotMutationCommand& setValue(const T& value_);

    /**
     * Set the value buffers (IO vectors) for the command.
     * Unlike #setValue() this does not copy the value to the command object
     *
     * @param bufs Buffers containing the value. The buffers should be
     *        considered owned by the command object until it is sent
     *        over the wire
     */
    template <typename T>
    BinprotMutationCommand& setValueBuffers(const T& bufs);

    BinprotMutationCommand& addValueBuffer(cb::const_byte_buffer buf);

    BinprotMutationCommand& setDatatype(uint8_t datatype_);
    BinprotMutationCommand& setDatatype(cb::mcbp::Datatype datatype_);
    BinprotMutationCommand& setDocumentFlags(uint32_t flags_);
    BinprotMutationCommand& setExpiry(uint32_t expiry_);

    void encode(std::vector<uint8_t>& buf) const override;
    Encoded encode() const override;

private:
    void encodeHeader(std::vector<uint8_t>& buf) const;

    /** This contains our copied value (i.e. setValue) */
    std::vector<uint8_t> value;
    /** This contains value references (e.g. addValueBuffer/setValueBuffers) */
    std::vector<cb::const_byte_buffer> value_refs;

    BinprotCommand::ExpiryValue expiry;
    uint32_t flags = 0;
    uint8_t datatype = 0;
};

class BinprotMutationResponse : public BinprotResponse {
public:
    void assign(std::vector<uint8_t>&& buf) override;

    const MutationInfo& getMutationInfo() const;

private:
    MutationInfo mutation_info;
};

class BinprotIncrDecrCommand : public BinprotCommandT<BinprotIncrDecrCommand> {
public:
    BinprotIncrDecrCommand& setDelta(uint64_t delta_);

    BinprotIncrDecrCommand& setInitialValue(uint64_t initial_);

    BinprotIncrDecrCommand& setExpiry(uint32_t expiry_);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint64_t delta = 0;
    uint64_t initial = 0;
    BinprotCommand::ExpiryValue expiry;
};

class BinprotIncrDecrResponse : public BinprotMutationResponse {
public:
    uint64_t getValue() const;

    void assign(std::vector<uint8_t>&& buf) override;

private:
    uint64_t value = 0;
};

class BinprotRemoveCommand
    : public BinprotCommandT<BinprotRemoveCommand,
                             cb::mcbp::ClientOpcode::Delete> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

using BinprotRemoveResponse = BinprotMutationResponse;

class BinprotGetErrorMapCommand
    : public BinprotCommandT<BinprotGetErrorMapCommand,
                             cb::mcbp::ClientOpcode::GetErrorMap> {
public:
    void setVersion(uint16_t version_);

    void encode(std::vector<uint8_t>& buf) const override;
private:
    uint16_t version = 0;
};

using BinprotGetErrorMapResponse = BinprotResponse;

class BinprotDcpOpenCommand : public BinprotGenericCommand {
public:
    /**
     * DCP Open
     *
     * @param name the name of the DCP stream to create
     * @param seqno_ the sequence number for the stream
     * @param flags_ the open flags
     */
    explicit BinprotDcpOpenCommand(const std::string& name,
                                   uint32_t seqno_ = 0,
                                   uint32_t flags_ = 0);

    /**
     * Make this a producer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeProducer();

    /**
     * Make this a consumer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeConsumer();

    /**
     * Let the stream include xattrs (if any)
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeIncludeXattr();

    /**
     * Don't add any values into the stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeNoValue();

    /**
     * Set an arbitrary flag value. This may be used in order to test
     * the sanity checks on the server
     *
     * @param flags the raw 32 bit flag section to inject
     * @return this
     */
    BinprotDcpOpenCommand& setFlags(uint32_t flags);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint32_t seqno;
    uint32_t flags;
};

class BinprotDcpStreamRequestCommand : public BinprotGenericCommand {
public:
    BinprotDcpStreamRequestCommand();

    BinprotDcpStreamRequestCommand& setDcpFlags(uint32_t value);

    BinprotDcpStreamRequestCommand& setDcpReserved(uint32_t value);

    BinprotDcpStreamRequestCommand& setDcpStartSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpEndSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpVbucketUuid(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpSnapStartSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpSnapEndSeqno(uint64_t value);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    // The byteorder is fixed when we append the members to the packet
    uint32_t dcp_flags;
    uint32_t dcp_reserved;
    uint64_t dcp_start_seqno;
    uint64_t dcp_end_seqno;
    uint64_t dcp_vbucket_uuid;
    uint64_t dcp_snap_start_seqno;
    uint64_t dcp_snap_end_seqno;
};

class BinprotGetFailoverLogCommand : public BinprotGenericCommand {
public:
    BinprotGetFailoverLogCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetFailoverLog){};
};

class BinprotSetParamCommand
    : public BinprotGenericCommand {
public:
    BinprotSetParamCommand(cb::mcbp::request::SetParamPayload::Type type_,
                           const std::string& key_,
                           const std::string& value_);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const cb::mcbp::request::SetParamPayload::Type type;
    const std::string value;
};

class BinprotSetWithMetaCommand
    : public BinprotGenericCommand {
public:
    BinprotSetWithMetaCommand(const Document& doc,
                              Vbid vbucket,
                              uint64_t operationCas,
                              uint64_t seqno,
                              uint32_t options,
                              std::vector<uint8_t>& meta);

    BinprotSetWithMetaCommand& setQuiet(bool quiet);

    uint32_t getFlags() const;

    BinprotSetWithMetaCommand& setFlags(uint32_t flags);

    uint32_t getExptime() const;

    BinprotSetWithMetaCommand& setExptime(uint32_t exptime);

    uint64_t getSeqno() const;

    BinprotSetWithMetaCommand& setSeqno(uint64_t seqno);

    uint64_t getMetaCas() const;

    BinprotSetWithMetaCommand& setMetaCas(uint64_t cas);

    const std::vector<uint8_t>& getMeta();

    BinprotSetWithMetaCommand& setMeta(const std::vector<uint8_t>& meta);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    Document doc;
    uint64_t seqno;
    uint64_t operationCas;
    uint32_t options;
    std::vector<uint8_t> meta;
};

class BinprotSetControlTokenCommand : public BinprotGenericCommand {
public:
    BinprotSetControlTokenCommand(uint64_t token_, uint64_t oldtoken);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const uint64_t token;
};

class BinprotSetClusterConfigCommand : public BinprotGenericCommand {
public:
    BinprotSetClusterConfigCommand(uint64_t token_, const std::string& config_);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const std::string config;
};

class BinprotObserveSeqnoCommand : public BinprotGenericCommand {
public:
    BinprotObserveSeqnoCommand(Vbid vbid, uint64_t uuid);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint64_t uuid;
};

class BinprotObserveSeqnoResponse : public BinprotResponse {
public:
    void assign(std::vector<uint8_t>&& buf) override;

    ObserveInfo info;
};

class BinprotUpdateUserPermissionsCommand : public BinprotGenericCommand {
public:
    BinprotUpdateUserPermissionsCommand(std::string payload);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const std::string payload;
};

using BinprotAuthProviderCommand =
        BinprotCommandT<BinprotGenericCommand,
                        cb::mcbp::ClientOpcode::AuthProvider>;

using BinprotRbacRefreshCommand =
        BinprotCommandT<BinprotGenericCommand,
                        cb::mcbp::ClientOpcode::RbacRefresh>;

class BinprotAuditPutCommand : public BinprotGenericCommand {
public:
    BinprotAuditPutCommand(uint32_t id, std::string payload);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const uint32_t id;
    const std::string payload;
};
