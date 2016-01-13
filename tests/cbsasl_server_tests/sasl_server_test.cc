/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"
#include "cbsasl/pwfile.h"
#include "cbsasl/util.h"
#include <cbsasl/cbsasl.h>
#include "cbsasl/cbsasl_internal.h"

#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <platform/platform.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#define DIGEST_LENGTH 16

const char* cbpwfile = "sasl_server_test.pw";

char envptr[256]{"ISASL_PWFILE=sasl_server_test.pw"};

class SaslServerTest : public ::testing::Test {
protected:
    void SetUp() {
        ASSERT_EQ(CBSASL_OK, cbsasl_server_init(nullptr,
                                                "cbsasl_server_test"));
    }

    void TearDown() {
        ASSERT_EQ(CBSASL_OK, cbsasl_server_term());
    }

    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        fprintf(fp, "mikewied mikepw \ncseo cpw \njlim jpw \nnopass\n");
        ASSERT_EQ(0, fclose(fp));

        putenv(envptr);
    }

    static void TearDownTestCase() {
        ASSERT_EQ(0, remove(cbpwfile));
        free_user_ht();
    }

protected:
    static void construct_cram_md5_credentials(char* buffer,
                                               unsigned* bufferlen,
                                               const char* user,
                                               unsigned userlen,
                                               const char* pass,
                                               unsigned passlen,
                                               const char* challenge,
                                               unsigned challengelen) {
        unsigned char digest[DIGEST_LENGTH];
        memcpy(buffer, user, userlen);
        buffer[userlen + 1] = ' ';

        unsigned int digest_len;
        if (HMAC(EVP_md5(), (unsigned char*)pass, passlen,
                 (unsigned char*)challenge, challengelen,
                 digest, &digest_len) == NULL || digest_len != DIGEST_LENGTH) {
            FAIL() << "HMAC md5 failed";
        }

        cbsasl_hex_encode(buffer + userlen + 1, (char*)digest, DIGEST_LENGTH);
        *bufferlen = 1 + (DIGEST_LENGTH * 2) + userlen;
    }
};

TEST_F(SaslServerTest, ListMechs) {
    const char* mechs = nullptr;
    unsigned len = 0;
    cbsasl_error_t err = cbsasl_listmech(nullptr, nullptr, nullptr, " ",
                                         nullptr, &mechs, &len, nullptr);
    ASSERT_EQ(CBSASL_OK, err);

    std::string mechanisms(mechs, len);
    std::string expected;

#ifdef HAVE_PKCS5_PBKDF2_HMAC
    expected.append("SCRAM-SHA512 SCRAM-SHA256 ");
#endif

#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
    expected.append("SCRAM-SHA1 ");
#endif
    expected.append("CRAM-MD5 PLAIN");

    EXPECT_EQ(expected, mechanisms);
}

TEST_F(SaslServerTest, ListMechsBadParam) {
    const char* mechs = nullptr;
    unsigned len = 0;
    cbsasl_error_t err = cbsasl_listmech(nullptr, nullptr, nullptr, ",",
                                         nullptr, &mechs, &len, nullptr);
    ASSERT_EQ(CBSASL_BADPARAM, err);
}

TEST_F(SaslServerTest, ListMechsSpecialized) {
    cbsasl_conn_t* conn;

    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));

    const char* mechs = nullptr;
    unsigned len = 0;
    int num;
    cbsasl_error_t err = cbsasl_listmech(conn, nullptr, "(", ",",
                                         ")", &mechs, &len, &num);
    ASSERT_EQ(CBSASL_OK, err);
    EXPECT_EQ(2, num);
    std::string mechanisms(mechs, len);
    std::string expected("(");

#ifdef HAVE_PKCS5_PBKDF2_HMAC
    expected.append("SCRAM-SHA512,SCRAM-SHA256,");
#endif

#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
    expected.append("SCRAM-SHA1,");
#endif
    expected.append("CRAM-MD5,PLAIN)");
    EXPECT_EQ(expected, mechanisms);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, BadMech) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    cbsasl_error_t err = cbsasl_server_start(conn, "bad_mech", nullptr, 0,
                                             nullptr, nullptr);
    ASSERT_EQ(CBSASL_BADPARAM, err);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainCorrectPassword) {
    /* Normal behavior */
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;
    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "\0mikewied\0mikepw", 16, &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainWrongPassword) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "\0mikewied\0badpPW", 16, &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_PWERR, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainNoPassword) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "\0nopass\0", 8,
                                             &output, &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainWithAuthzid) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "funzid\0mikewied\0mikepw", 22,
                                             &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainWithNoPwOrUsernameEndingNull) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "funzid\0mikewied",
                                             15, &output, &outputlen);
    ASSERT_NE(CBSASL_OK, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, PlainNoNullAtAll) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "funzidmikewied",
                                             14, &output, &outputlen);
    ASSERT_NE(CBSASL_OK, err);
    free((void*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, CramMD5) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* challenge = nullptr;
    unsigned challengelen = 0;

    ASSERT_EQ(CBSASL_CONTINUE,
              cbsasl_server_start(conn, "CRAM-MD5", nullptr, 0, &challenge,
                                  &challengelen));

    const char* user = "mikewied";
    const char* pass = "mikepw";
    char creds[128];
    unsigned credslen = 0;
    construct_cram_md5_credentials(creds, &credslen, user,
                                   (unsigned int)strlen(user), pass,
                                   (unsigned int)strlen(pass),
                                   (const char* )challenge, challengelen);
    const char *output;
    unsigned outputlen;

    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_step(conn, creds, credslen, &output, &outputlen));
    free((char*)output);
    cbsasl_dispose(&conn);
}

TEST_F(SaslServerTest, CramMD5WrongPassword) {
    cbsasl_conn_t* conn = nullptr;
    ASSERT_EQ(CBSASL_OK,
              cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                nullptr, 0, &conn));
    ASSERT_NE(nullptr, conn);
    unsigned char* challenge = nullptr;
    unsigned challengelen = 0;
    ASSERT_EQ(CBSASL_CONTINUE,
              cbsasl_server_start(conn, "CRAM-MD5", nullptr, 0, &challenge,
                                  &challengelen));

    const char* user = "mikewied";
    const char* pass = "padpw";
    char creds[128];
    unsigned credslen = 0;
    const char* output = NULL;
    unsigned outputlen = 0;
    construct_cram_md5_credentials(creds, &credslen, user,
                                   (unsigned int)strlen(user), pass,
                                   (unsigned int)strlen(pass),
                                   (const char* )challenge, challengelen);

    ASSERT_EQ(CBSASL_PWERR,
              cbsasl_server_step(conn, creds, credslen, &output, &outputlen));
    free((char*)output);
    cbsasl_dispose(&conn);
}
