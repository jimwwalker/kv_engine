/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <daemon/settings.h>
#include <getopt.h>
#include <gtest/gtest.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

class SettingsTest : public ::testing::Test {
public:
    /**
     * Test that all values except a string value throws an exception.
     * @param tag
     */
    void nonStringValuesShouldFail(const std::string& tag);

    /**
     * Test that all values except boolean values throws an exception.
     * @param tag
     */
    void nonBooleanValuesShouldFail(const std::string& tag);

    /**
     * Test that all values except numeric values throws an exception.
     * @param tag
     */
    void nonNumericValuesShouldFail(const std::string& tag);

    void nonArrayValuesShouldFail(const std::string& tag);

    void nonObjectValuesShouldFail(const std::string& tag);

    /**
     * Convenience method - returns a config JSON object with an "interfaces"
     * array containing single interface object with the given properties.
     */
    nlohmann::json makeInterfacesConfig(const char* protocolMode);
    template <typename T = nlohmann::json::exception>
    void expectFail(const nlohmann::json& json) {
        EXPECT_THROW(Settings settings(json), T) << json.dump();
    }
};

void SettingsTest::nonStringValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonBooleanValuesShouldFail(const std::string& tag) {
    // String values should not be accepted
    nlohmann::json json;
    json[tag] = "foo";
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonNumericValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonArrayValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

void SettingsTest::nonObjectValuesShouldFail(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);
}

nlohmann::json SettingsTest::makeInterfacesConfig(const char* protocolMode) {
    nlohmann::json obj;
    obj["ipv4"] = protocolMode;
    obj["ipv6"] = protocolMode;

    nlohmann::json arr;
    arr.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = arr;
    return root;
}

TEST_F(SettingsTest, AuditFile) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("audit_file");

    char pattern[] = {"audit_file.XXXXXX"};

    // Ensure that we accept a string, but the file must exist
    EXPECT_NE(nullptr, cb_mktemp(pattern));

    nlohmann::json json;
    json["audit_file"] = pattern;
    try {
        Settings settings(json);
        EXPECT_EQ(pattern, settings.getAuditFile());
        EXPECT_TRUE(settings.has.audit);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    cb::io::rmrf(pattern);
    expectFail<std::system_error>(json);
}

TEST_F(SettingsTest, RbacFile) {
    nonStringValuesShouldFail("rbac_file");

    const std::string pattern = {"rbac_file.XXXXXX"};

    // Ensure that we accept a string, but the file must exist
    const auto tmpfile = cb::io::mktemp(pattern);

    nlohmann::json json;
    json["rbac_file"] = tmpfile;
    try {
        Settings settings(json);
        EXPECT_EQ(tmpfile, settings.getRbacFile());
        EXPECT_TRUE(settings.has.rbac_file);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    cb::io::rmrf(tmpfile);
    expectFail<std::system_error>(json);
}

TEST_F(SettingsTest, Threads) {
    nonNumericValuesShouldFail("threads");

    nlohmann::json json;
    // Explicitly make threads an unsigned int
    uint8_t threads = 10;
    json["threads"] = threads;
    try {
        Settings settings(json);
        EXPECT_EQ(10, settings.getNumWorkerThreads());
        EXPECT_TRUE(settings.has.threads);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Interfaces) {
    nonArrayValuesShouldFail("interfaces");

    char key_pattern[] = {"key.XXXXXX"};
    char cert_pattern[] = {"cert.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(key_pattern));
    EXPECT_NE(nullptr, cb_mktemp(cert_pattern));

    nlohmann::json obj;
    obj["port"] = 0;
    obj["ipv4"] = true;
    obj["ipv6"] = true;
    obj["maxconn"] = 10;
    obj["backlog"] = 10;
    obj["host"] = "*";
    obj["protocol"] = "memcached";
    obj["management"] = true;

    nlohmann::json ssl;
    ssl["key"] = key_pattern;
    ssl["cert"] = cert_pattern;
    obj["ssl"] = ssl;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    try {
        Settings settings(root);
        EXPECT_EQ(1, settings.getInterfaces().size());
        EXPECT_TRUE(settings.has.interfaces);

        const auto& ifc0 = settings.getInterfaces()[0];

        EXPECT_EQ(0, ifc0.port);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv6);
        EXPECT_EQ(10, ifc0.maxconn);
        EXPECT_EQ(10, ifc0.backlog);
        EXPECT_EQ("*", ifc0.host);
        EXPECT_TRUE(ifc0.management);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, InterfacesMissingSSLFiles) {
    nonArrayValuesShouldFail("interfaces");

    char key_pattern[] = {"key.XXXXXX"};
    char cert_pattern[] = {"cert.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(key_pattern));
    EXPECT_NE(nullptr, cb_mktemp(cert_pattern));

    nlohmann::json obj;
    obj["port"] = 0;
    obj["ipv4"] = true;
    obj["ipv6"] = true;
    obj["maxconn"] = 10;
    obj["backlog"] = 10;
    obj["host"] = "*";
    obj["protocol"] = "memcached";
    obj["management"] = true;

    nlohmann::json ssl;
    ssl["key"] = key_pattern;
    ssl["cert"] = cert_pattern;
    obj["ssl"] = ssl;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    try {
        Settings settings(root);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // We should fail if one of the files is missing
    cb::io::rmrf(key_pattern);
    expectFail<std::system_error>(root);
    fclose(fopen(key_pattern, "a"));
    cb::io::rmrf(cert_pattern);
    expectFail<std::system_error>(root);
    cb::io::rmrf(key_pattern);
}

TEST_F(SettingsTest, InterfacesInvalidSslEntry) {
    nonArrayValuesShouldFail("interfaces");

    char pattern[] = {"ssl.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(pattern));

    nlohmann::json obj;
    obj["port"] = 0;
    obj["ipv4"] = true;
    obj["ipv6"] = true;
    obj["maxconn"] = 10;
    obj["backlog"] = 10;
    obj["host"] = "*";
    obj["protocol"] = "memcached";
    obj["management"] = true;

    nlohmann::json ssl;
    ssl["cert"] = pattern;
    obj["ssl"] = ssl;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    expectFail<nlohmann::json::exception>(root);

    ssl.clear();
    ssl["key"] = pattern;
    obj["ssl"] = ssl;
    array.clear();
    array.push_back(obj);
    root["interfaces"] = array;

    expectFail<nlohmann::json::exception>(root);

    cb::io::rmrf(pattern);
}

/// Test that "off" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolOff) {
    auto root = makeInterfacesConfig("off");

    try {
        Settings settings(root);
        ASSERT_EQ(1, settings.getInterfaces().size());
        ASSERT_TRUE(settings.has.interfaces);

        const auto& ifc0 = settings.getInterfaces()[0];

        EXPECT_EQ(NetworkInterface::Protocol::Off, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Off, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that "optional" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolOptional) {
    auto root = makeInterfacesConfig("optional");

    try {
        Settings settings(root);
        ASSERT_EQ(1, settings.getInterfaces().size());
        ASSERT_TRUE(settings.has.interfaces);

        const auto& ifc0 = settings.getInterfaces()[0];

        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Optional, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that "required" is correctly handled for ipv4 & ipv6 protocols.
TEST_F(SettingsTest, InterfacesProtocolRequired) {
    auto root = makeInterfacesConfig("required");

    try {
        Settings settings(root);
        ASSERT_EQ(1, settings.getInterfaces().size());
        ASSERT_TRUE(settings.has.interfaces);

        const auto& ifc0 = settings.getInterfaces()[0];

        EXPECT_EQ(NetworkInterface::Protocol::Required, ifc0.ipv4);
        EXPECT_EQ(NetworkInterface::Protocol::Required, ifc0.ipv6);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

/// Test that invalid numeric values for ipv4 & ipv6 protocols are rejected.
TEST_F(SettingsTest, InterfacesInvalidProtocolNumber) {
    // Numbers not permitted
    nlohmann::json obj;
    obj["ipv4"] = 1;
    obj["ipv6"] = 2;

    nlohmann::json array;
    array.push_back(obj);

    nlohmann::json root;
    root["interfaces"] = array;

    expectFail<nlohmann::json::exception>(root);
}

/// Test that invalid string values for ipv4 & ipv6 protocols are rejected.
TEST_F(SettingsTest, InterfacesInvalidProtocolString) {
    // Strings not in (off, optional, required) not permitted.
    auto root = makeInterfacesConfig("sometimes");

    expectFail<std::invalid_argument>(root);
}

TEST_F(SettingsTest, ParseLoggerSettings) {
    nonObjectValuesShouldFail("logger");

    nlohmann::json obj;
    obj["filename"] = "logs/n_1/memcached.log";
    obj["buffersize"] = 1024;
    obj["cyclesize"] = 10485760;
    obj["unit_test"] = true;

    nlohmann::json root;
    root["logger"] = obj;
    Settings settings(root);

    EXPECT_TRUE(settings.has.logger);

    const auto config = settings.getLoggerConfig();
    EXPECT_EQ("logs/n_1/memcached.log", config.filename);
    EXPECT_EQ(1024, config.buffersize);
    EXPECT_EQ(10485760, config.cyclesize);
    EXPECT_EQ(true, config.unit_test);
}

TEST_F(SettingsTest, StdinListener) {
    nonBooleanValuesShouldFail("stdin_listener");

    nlohmann::json obj;
    obj["stdin_listener"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isStdinListenerEnabled());
        EXPECT_TRUE(settings.has.stdin_listener);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["stdin_listener"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isStdinListenerEnabled());
        EXPECT_TRUE(settings.has.stdin_listener);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, TopkeysEnabled) {
    nonBooleanValuesShouldFail("topkeys_enabled");

    nlohmann::json obj;
    obj["topkeys_enabled"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isTopkeysEnabled());
        EXPECT_TRUE(settings.has.topkeys_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["topkeys_enabled"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isTopkeysEnabled());
        EXPECT_TRUE(settings.has.topkeys_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DefaultReqsPerEvent) {
    nonNumericValuesShouldFail("default_reqs_per_event");

    nlohmann::json obj;
    // Explicitly make defaultReqsPerEvent an unsigned int
    uint8_t defaultReqsPerEvent = 10;
    obj["default_reqs_per_event"] = defaultReqsPerEvent;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(
                          EventPriority::Default));
        EXPECT_TRUE(settings.has.default_reqs_per_event);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, HighPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_high_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventHighPriority an unsigned int
    uint8_t reqsPerEventHighPriority = 10;
    obj["reqs_per_event_high_priority"] = reqsPerEventHighPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(
                10,
                settings.getRequestsPerEventNotification(EventPriority::High));
        EXPECT_TRUE(settings.has.reqs_per_event_high_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, MediumPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_med_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventMedPriority an unsigned int
    uint8_t reqsPerEventMedPriority = 10;
    obj["reqs_per_event_med_priority"] = reqsPerEventMedPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(
                          EventPriority::Medium));
        EXPECT_TRUE(settings.has.reqs_per_event_med_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, LowPriorityReqsPerEvent) {
    nonNumericValuesShouldFail("reqs_per_event_low_priority");

    nlohmann::json obj;
    // Explicitly make reqsPerEventLowPriority an unsigned int
    uint8_t reqsPerEventLowPriority = 10;
    obj["reqs_per_event_low_priority"] = reqsPerEventLowPriority;
    try {
        Settings settings(obj);
        EXPECT_EQ(10,
                  settings.getRequestsPerEventNotification(EventPriority::Low));
        EXPECT_TRUE(settings.has.reqs_per_event_low_priority);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Verbosity) {
    nonNumericValuesShouldFail("verbosity");

    nlohmann::json obj;
    // Explicitly make verbosity an unsigned int
    uint8_t verbosity = 1;
    obj["verbosity"] = verbosity;
    try {
        Settings settings(obj);
        EXPECT_EQ(1, settings.getVerbose());
        EXPECT_TRUE(settings.has.verbose);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ConnectionIdleTime) {
    nonNumericValuesShouldFail("connection_idle_time");

    nlohmann::json obj;
    // Explicitly make connection_idle_time an unsigned int
    uint16_t connectionIdleTime = 500;
    obj["connection_idle_time"] = connectionIdleTime;
    try {
        Settings settings(obj);
        EXPECT_EQ(500, settings.getConnectionIdleTime());
        EXPECT_TRUE(settings.has.connection_idle_time);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, BioDrainBufferSize) {
    nonNumericValuesShouldFail("bio_drain_buffer_sz");

    nlohmann::json obj;
    // Explicitly make bioDrainBufferSize an unsigned int
    uint16_t bioDrainBufferSize = 1024;
    obj["bio_drain_buffer_sz"] = bioDrainBufferSize;
    try {
        Settings settings(obj);
        EXPECT_EQ(1024, settings.getBioDrainBufferSize());
        EXPECT_TRUE(settings.has.bio_drain_buffer_sz);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DatatypeJson) {
    nonBooleanValuesShouldFail("datatype_json");

    nlohmann::json obj;
    obj["datatype_json"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDatatypeJsonEnabled());
        EXPECT_TRUE(settings.has.datatype_json);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["datatype_json"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDatatypeJsonEnabled());
        EXPECT_TRUE(settings.has.datatype_json);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DatatypeSnappy) {
    nonBooleanValuesShouldFail("datatype_snappy");

    nlohmann::json obj;
    obj["datatype_snappy"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDatatypeSnappyEnabled());
        EXPECT_TRUE(settings.has.datatype_snappy);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["datatype_snappy"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDatatypeSnappyEnabled());
        EXPECT_TRUE(settings.has.datatype_snappy);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, Root) {
    // Ensure that we detect non-string values for admin
    nonStringValuesShouldFail("root");

    // Ensure that we accept a string, but it must be a directory
    nlohmann::json obj;
    obj["root"] = "/";
    try {
        Settings settings(obj);
        EXPECT_EQ("/", settings.getRoot());
        EXPECT_TRUE(settings.has.root);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But we should fail if the file don't exist
    obj["root"] = "/it/would/suck/if/this/exist";
    expectFail<std::system_error>(obj);
}

TEST_F(SettingsTest, SslCipherList) {
    // Ensure that we detect non-string values for ssl_cipher_list
    nonStringValuesShouldFail("ssl_cipher_list");

    // Ensure that we accept a string
    nlohmann::json obj;
    obj["ssl_cipher_list"] = "HIGH";
    try {
        Settings settings(obj);
        EXPECT_EQ("HIGH", settings.getSslCipherList());
        EXPECT_TRUE(settings.has.ssl_cipher_list);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj["ssl_cipher_list"] = "";
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSslCipherList());
        EXPECT_TRUE(settings.has.ssl_cipher_list);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, SslMinimumProtocol) {
    nonStringValuesShouldFail("ssl_minimum_protocol");

    nlohmann::json obj;
    const std::vector<std::string> protocol = {"tlsv1", "tlsv1.1", "tlsv1_1",
                                               "tlsv1.2", "tlsv1_2"};
    for (const auto& p : protocol) {
        // Ensure that we accept a string
        obj["ssl_minimum_protocol"] = p;
        try {
            Settings settings(obj);
            EXPECT_EQ(p, settings.getSslMinimumProtocol());
            EXPECT_TRUE(settings.has.ssl_minimum_protocol);
        } catch (std::exception& exception) {
            FAIL() << exception.what();
        }
    }

    // An empty string is also allowed
    obj["ssl_minimum_protocol"] = "";
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSslMinimumProtocol());
        EXPECT_TRUE(settings.has.ssl_minimum_protocol);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // But random strings shouldn't be allowed
    obj["ssl_minimum_protocol"] = "foo";
    expectFail<std::invalid_argument>(obj);
}

TEST_F(SettingsTest, Breakpad) {
    nonObjectValuesShouldFail("breakpad");

    nlohmann::json json;

    char minidump_dir[] = {"minidump.XXXXXX"};
    EXPECT_NE(nullptr, cb_mktemp(minidump_dir));
    cb::io::rmrf(minidump_dir);
    cb::io::mkdirp(minidump_dir);

    json["enabled"] = true;
    json["minidump_dir"] = minidump_dir;

    // Content is optional
    EXPECT_NO_THROW(cb::breakpad::Settings settings(json));

    // But the minidump dir is mandatory
    cb::io::rmrf(minidump_dir);
    EXPECT_THROW(cb::breakpad::Settings settings(json), std::system_error);
    cb::io::mkdirp(minidump_dir);

    json["content"] = "default";
    EXPECT_NO_THROW(cb::breakpad::Settings settings(json));
    json["content"] = "foo";
    EXPECT_THROW(cb::breakpad::Settings settings(json), std::invalid_argument);

    cb::io::rmrf(minidump_dir);
}

TEST_F(SettingsTest, max_packet_size) {
    nonNumericValuesShouldFail("max_packet_size");

    nlohmann::json obj;
    // the config file specifies it in MB, we're keeping it as bytes internally
    // Explicitly make maxPacketSize and unsigned int
    uint8_t maxPacketSize = 30;
    obj["max_packet_size"] = maxPacketSize;
    try {
        Settings settings(obj);
        EXPECT_EQ(30 * 1024 * 1024, settings.getMaxPacketSize());
        EXPECT_TRUE(settings.has.max_packet_size);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, SaslMechanisms) {
    nonStringValuesShouldFail("sasl_mechanisms");

    // Ensure that we accept a string
    nlohmann::json obj;
    obj["sasl_mechanisms"] = "SCRAM-SHA1";
    try {
        Settings settings(obj);
        EXPECT_EQ("SCRAM-SHA1", settings.getSaslMechanisms());
        EXPECT_TRUE(settings.has.sasl_mechanisms);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    // An empty string is also allowed
    obj["sasl_mechanisms"] = "";
    try {
        Settings settings(obj);
        EXPECT_EQ("", settings.getSaslMechanisms());
        EXPECT_TRUE(settings.has.sasl_mechanisms);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, DedupeNmvbMaps) {
    nonBooleanValuesShouldFail("dedupe_nmvb_maps");

    nlohmann::json obj;
    obj["dedupe_nmvb_maps"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["dedupe_nmvb_maps"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isDedupeNmvbMaps());
        EXPECT_TRUE(settings.has.dedupe_nmvb_maps);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, XattrEnabled) {
    nonBooleanValuesShouldFail("xattr_enabled");

    nlohmann::json obj;
    obj["xattr_enabled"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isXattrEnabled());
        EXPECT_TRUE(settings.has.xattr_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["xattr_enabled"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isXattrEnabled());
        EXPECT_TRUE(settings.has.xattr_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, TracingEnabled) {
    nonBooleanValuesShouldFail("tracing_enabled");

    nlohmann::json obj;
    obj["tracing_enabled"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isTracingEnabled());
        EXPECT_TRUE(settings.has.tracing_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["tracing_enabled"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isTracingEnabled());
        EXPECT_TRUE(settings.has.tracing_enabled);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ExternalAuthService) {
    nonBooleanValuesShouldFail("external_auth_service");

    nlohmann::json obj;
    obj["external_auth_service"] = true;
    try {
        Settings settings(obj);
        EXPECT_TRUE(settings.isExternalAuthServiceEnabled());
        EXPECT_TRUE(settings.has.external_auth_service);
    } catch (const std::exception& exception) {
        FAIL() << exception.what();
    }

    obj["external_auth_service"] = false;
    try {
        Settings settings(obj);
        EXPECT_FALSE(settings.isExternalAuthServiceEnabled());
        EXPECT_TRUE(settings.has.external_auth_service);
    } catch (const std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST_F(SettingsTest, ScramshaFallbackSalt) {
    nonStringValuesShouldFail("scramsha_fallback_salt");
    nlohmann::json obj;
    obj["scramsha_fallback_salt"] = "JKouEmqRFI+Re/AA";
    try {
        Settings settings(obj);
        EXPECT_EQ("JKouEmqRFI+Re/AA", settings.getScramshaFallbackSalt());
        EXPECT_TRUE(settings.has.scramsha_fallback_salt);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

TEST(SettingsUpdateTest, EmptySettingsShouldWork) {
    Settings updated;
    Settings settings;
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, RootIsNotDynamic) {
    Settings settings;
    settings.setRoot("/tmp");
    // setting it to the same value should work
    Settings updated;
    updated.setRoot(settings.getRoot());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setRoot("/var");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, BreakpadIsDynamic) {
    Settings updated;
    Settings settings;
    cb::breakpad::Settings breakpadSettings;
    breakpadSettings.enabled = true;
    breakpadSettings.minidump_dir = "/var/crash";

    settings.setBreakpadSettings(breakpadSettings);
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    breakpadSettings.enabled = false;
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_TRUE(settings.getBreakpadSettings().enabled);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_FALSE(settings.getBreakpadSettings().enabled);

    breakpadSettings.minidump_dir = "/var/crash/minidump";
    updated.setBreakpadSettings(breakpadSettings);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ("/var/crash", settings.getBreakpadSettings().minidump_dir);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("/var/crash/minidump",
              settings.getBreakpadSettings().minidump_dir);
    EXPECT_FALSE(settings.getBreakpadSettings().enabled);
}

TEST(SettingsUpdateTest, AuditFileIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setAuditFile("/etc/opt/couchbase/etc/security/audit.json");
    updated.setAuditFile(settings.getAuditFile());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setAuditFile("/opt/couchbase/etc/security/audit.json");
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, ThreadsIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setNumWorkerThreads(4);
    updated.setNumWorkerThreads(settings.getNumWorkerThreads());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should fail
    updated.setNumWorkerThreads(settings.getNumWorkerThreads() - 1);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, InterfaceIdenticalArraysShouldWork) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    NetworkInterface ifc;
    ifc.host.assign("*");
    ifc.ssl.key.assign("/etc/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/etc/opt/couchbase/security/cert.pem");

    updated.addInterface(ifc);
    settings.addInterface(ifc);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, InterfaceSomeValuesMayChange) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    NetworkInterface ifc;
    ifc.host.assign("*");
    ifc.ssl.key.assign("/etc/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/etc/opt/couchbase/security/cert.pem");

    settings.addInterface(ifc);

    ifc.backlog = 10;
    ifc.maxconn = 10;
    ifc.tcp_nodelay = false;
    ifc.ssl.key.assign("/opt/couchbase/security/key.pem");
    ifc.ssl.cert.assign("/opt/couchbase/security/cert.pem");

    updated.addInterface(ifc);

    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_NE(ifc.backlog, settings.getInterfaces()[0].backlog);
    EXPECT_NE(ifc.maxconn, settings.getInterfaces()[0].maxconn);
    EXPECT_NE(ifc.tcp_nodelay, settings.getInterfaces()[0].tcp_nodelay);
    EXPECT_NE(ifc.ssl.key, settings.getInterfaces()[0].ssl.key);
    EXPECT_NE(ifc.ssl.cert, settings.getInterfaces()[0].ssl.cert);

    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ifc.backlog, settings.getInterfaces()[0].backlog);
    EXPECT_EQ(ifc.maxconn, settings.getInterfaces()[0].maxconn);
    EXPECT_EQ(ifc.tcp_nodelay, settings.getInterfaces()[0].tcp_nodelay);
    EXPECT_EQ(ifc.ssl.key, settings.getInterfaces()[0].ssl.key);
    EXPECT_EQ(ifc.ssl.cert, settings.getInterfaces()[0].ssl.cert);
}

TEST(SettingsUpdateTest, InterfaceSomeValuesMayNotChange) {
    Settings settings;
    // setting it to the same value should work

    NetworkInterface ifc;

    settings.addInterface(ifc);

    {
        Settings updated;
        NetworkInterface myifc;
        myifc.host.assign("localhost");
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        NetworkInterface myifc;
        myifc.port = 11200;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        NetworkInterface myifc;
        myifc.ipv4 = NetworkInterface::Protocol::Off;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        NetworkInterface myifc;
        myifc.ipv6 = NetworkInterface::Protocol::Off;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }

    {
        Settings updated;
        NetworkInterface myifc;
        myifc.management = true;
        updated.addInterface(myifc);

        EXPECT_THROW(settings.updateSettings(updated, false),
                     std::invalid_argument);
    }
}

TEST(SettingsUpdateTest, InterfaceDifferentArraySizeShouldFail) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work

    NetworkInterface ifc;
    settings.addInterface(ifc);
    updated.addInterface(ifc);

    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    updated.addInterface(ifc);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
    settings.addInterface(ifc);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    settings.addInterface(ifc);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, UpdatingLoggerSettingsShouldFail) {
    Settings settings;
    Settings updated;

    cb::logger::Config config;
    config.filename.assign("logger_test");
    config.buffersize = 1024;
    config.cyclesize = 1024 * 1024;

    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    updated.setLoggerConfig(config);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, DefaultReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Default);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Default);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Default);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Default));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Default));
}

TEST(SettingsUpdateTest, HighPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::High);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::High);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::High);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::High));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::High));
}

TEST(SettingsUpdateTest, MedPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Medium);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Medium);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Medium);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Medium));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Medium));
}

TEST(SettingsUpdateTest, LowPriReqIsDynamic) {
    Settings updated;
    Settings settings;
    settings.setRequestsPerEventNotification(10, EventPriority::Low);
    // setting it to the same value should work
    int ii = 10;
    updated.setRequestsPerEventNotification(ii, EventPriority::Low);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    ii += 1000;
    updated.setRequestsPerEventNotification(ii, EventPriority::Low);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(ii - 1000,
              settings.getRequestsPerEventNotification(EventPriority::Low));
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(ii,
              settings.getRequestsPerEventNotification(EventPriority::Low));
}

TEST(SettingsUpdateTest, VerbosityIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getVerbose();
    updated.setVerbose(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setVerbose(settings.getVerbose() + 1);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getVerbose());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getVerbose(), settings.getVerbose());
}

TEST(SettingsUpdateTest, ConnectionIdleTimeIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getConnectionIdleTime();
    updated.setConnectionIdleTime(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setConnectionIdleTime(old + 10);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getConnectionIdleTime());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getConnectionIdleTime(),
              settings.getConnectionIdleTime());
}

TEST(SettingsUpdateTest, BioDrainBufferSzIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    auto old = settings.getBioDrainBufferSize();
    updated.setBioDrainBufferSize(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setBioDrainBufferSize(old + 10);
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, DatatypeJsonIsNotDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setDatatypeJsonEnabled(true);
    updated.setDatatypeJsonEnabled(settings.isDatatypeJsonEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should not work
    updated.setDatatypeJsonEnabled(!settings.isDatatypeJsonEnabled());
    EXPECT_THROW(settings.updateSettings(updated, false),
                 std::invalid_argument);
}

TEST(SettingsUpdateTest, DatatypeSnappyIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setDatatypeSnappyEnabled(true);
    updated.setDatatypeSnappyEnabled(
            settings.isDatatypeSnappyEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setDatatypeSnappyEnabled(
            !settings.isDatatypeSnappyEnabled());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
}

TEST(SettingsUpdateTest, SslCipherListIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setSslCipherList("high");
    auto old = settings.getSslCipherList();
    updated.setSslCipherList(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setSslCipherList("low");
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getSslCipherList());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("low", settings.getSslCipherList());
}

TEST(SettingsUpdateTest, SslMinimumProtocolIsDynamic) {
    Settings updated;
    Settings settings;
    // setting it to the same value should work
    settings.setSslMinimumProtocol("tlsv1.2");
    auto old = settings.getSslMinimumProtocol();
    updated.setSslMinimumProtocol(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setSslMinimumProtocol("tlsv1");
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getSslMinimumProtocol());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ("tlsv1", settings.getSslMinimumProtocol());
}

TEST(SettingsUpdateTest, MaxPacketSizeIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    auto old = settings.getMaxPacketSize();
    updated.setMaxPacketSize(old);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // changing it should work
    updated.setMaxPacketSize(old + 10);
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_EQ(old, settings.getMaxPacketSize());
    EXPECT_NO_THROW(settings.updateSettings(updated));
    EXPECT_EQ(updated.getMaxPacketSize(),
              settings.getMaxPacketSize());
}

TEST(SettingsUpdateTest, SaslMechanismsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setSaslMechanisms("SCRAM-SHA1");
    updated.setSaslMechanisms(settings.getSaslMechanisms());
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setSaslMechanisms("PLAIN");
    settings.updateSettings(updated);
    EXPECT_EQ("PLAIN", settings.getSaslMechanisms());
}

TEST(SettingsUpdateTest, SslSaslMechanismsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setSslSaslMechanisms("SCRAM-SHA1");
    updated.setSslSaslMechanisms(settings.getSslSaslMechanisms());
    settings.updateSettings(updated, false);

    // changing it should work
    updated.setSslSaslMechanisms("PLAIN");
    settings.updateSettings(updated);
    EXPECT_EQ("PLAIN", settings.getSslSaslMechanisms());
}

TEST(SettingsUpdateTest, DedupeNmvbMapsIsDynamic) {
    Settings settings;
    Settings updated;
    // setting it to the same value should work
    settings.setDedupeNmvbMaps(true);
    updated.setDedupeNmvbMaps(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    updated.setDedupeNmvbMaps(!settings.isDedupeNmvbMaps());
    EXPECT_TRUE(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_TRUE(settings.isDedupeNmvbMaps());
    EXPECT_NO_THROW(settings.updateSettings(updated, true));
    EXPECT_FALSE(settings.isDedupeNmvbMaps());
}

TEST(SettingsUpdateTest, OpcodeAttributesOverrideIsDynamic) {
    Settings settings;
    Settings updated;

    // setting it to the same value should work
    settings.setOpcodeAttributesOverride(R"({"version":1})");
    updated.setOpcodeAttributesOverride(settings.getOpcodeAttributesOverride());
    EXPECT_NO_THROW(settings.updateSettings(updated, false));

    // Changing it should also work
    updated.setOpcodeAttributesOverride(R"({"version":1, "comment":"foo"})");

    // Dry-run
    EXPECT_NO_THROW(settings.updateSettings(updated, false));
    EXPECT_NE(updated.getOpcodeAttributesOverride(),
              settings.getOpcodeAttributesOverride());

    // with update
    EXPECT_NO_THROW(settings.updateSettings(updated, true));
    EXPECT_EQ(updated.getOpcodeAttributesOverride(),
              settings.getOpcodeAttributesOverride());
}

TEST(SettingsUpdateTest, OpcodeAttributesMustBeValidFormat) {
    Settings settings;

    // It must be json containing "version"
    EXPECT_THROW(settings.setOpcodeAttributesOverride("{}"),
                 std::invalid_argument);

    // it works if it contains a valid entry
    settings.setOpcodeAttributesOverride(
            R"({"version":1,"default": {"slow":500}})");

    // Setting to an empty value means drop the previous content
    settings.setOpcodeAttributesOverride("");
    EXPECT_EQ("", settings.getOpcodeAttributesOverride());
}

TEST_F(SettingsTest, ScramshaFallbackSaltIsDynamic) {
    Settings settings;
    Settings updated;

    // setting it to the same value should work
    settings.setScramshaFallbackSalt("Original");
    updated.setScramshaFallbackSalt("New");
    EXPECT_NO_THROW(settings.updateSettings(updated, true));

    try {
        EXPECT_EQ("New", settings.getScramshaFallbackSalt());
        EXPECT_TRUE(settings.has.scramsha_fallback_salt);
    } catch (std::exception& exception) {
        FAIL() << exception.what();
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    bool verbose = false;

    int cmd;
    while ((cmd = getopt(argc, argv, "v")) != EOF) {
        switch (cmd) {
        case 'v':
            verbose = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose memcached output "
                      << "to stderr." << std::endl;
            return EXIT_FAILURE;
        }
    }

    if (verbose) {
        cb::logger::createConsoleLogger();
    } else {
        cb::logger::createBlackholeLogger();
    }

    auto ret = RUN_ALL_TESTS();
    cb::logger::shutdown();

    return ret;
}
