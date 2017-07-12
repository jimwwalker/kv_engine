/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "output_couchfile.h"
#include "xattr_couchfile.h"

#include <cJSON.h>
#include <cJSON_utils.h>

OutputCouchFile::OutputCouchFile(OptionsSet options,
                                 const std::string& filename)
    : CouchFile(options, filename, COUCHSTORE_OPEN_FLAG_CREATE) {
    validNamespaces.push_back(DocNamespace::DefaultCollection);
    validNamespaces.push_back(DocNamespace::Collections);
    validNamespaces.push_back(DocNamespace::System);
}

void OutputCouchFile::commit() const {
    auto errcode = couchstore_commit(db);
    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::commit couchstore_commit failed errcode:" +
                std::to_string(errcode));
    }
    verbose("commit");
}

std::vector<char> OutputCouchFile::createNamespacedName(
        const sized_buf in, DocNamespace newNamespace) const {
    std::vector<char> rv(in.size + 1);
    rv[0] = char(newNamespace);
    std::copy(in.buf, in.buf + in.size, rv.begin() + 1);
    return rv;
}

static int mergeCallback(Db* db, DocInfo* docinfo, void* ctx) {
    DocPtr doc;
    auto errcode = couchstore_open_doc_with_docinfo(
            db, docinfo, doc.getDocAddress(), DECOMPRESS_DOC_BODIES);
    if (errcode) {
        throw std::runtime_error(
                "InputCouchFile::upgradeCallback "
                "couchstore_open_doc_with_docinfo errcode:" +
                std::to_string(errcode));
    } else {
        auto* output = reinterpret_cast<OutputCouchFile*>(ctx);
        output->writeDocument(doc.getDoc(), docinfo, false /*preserveSeqno*/);
    }
    return 0;
}

void OutputCouchFile::merge(const XattrCouchFile& input) {
    input.getAllDocs(&mergeCallback, this);
    validNamespaces.push_back(DocNamespace::UserXattrs);
    validNamespaces.push_back(DocNamespace::SystemXattrs);
}

void OutputCouchFile::processDocument(const Doc* doc,
                                      const DocInfo* docinfo,
                                      bool preserveSeqno) const {
    auto newName =
            createNamespacedName(doc->id, DocNamespace::DefaultCollection);
    Doc newDoc = *doc;
    DocInfo newDocInfo = *docinfo;
    newDoc.id.buf = newName.data();
    newDoc.id.size = newName.size();
    newDocInfo.id = newDoc.id;

    writeDocument(&newDoc, &newDocInfo, preserveSeqno);
}

void OutputCouchFile::writeDocument(const Doc* doc,
                                    const DocInfo* docinfo,
                                    bool preserveSeqno) const {
    couchstore_save_options saveOptions =
            preserveSeqno ? COUCHSTORE_SEQUENCE_AS_IS : 0;
    auto errcode = couchstore_save_document(db,
                                            doc,
                                            const_cast<DocInfo*>(docinfo),
                                            COMPRESS_DOC_BODIES | saveOptions);

    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::writeDocument couchstore_save_document "
                "errcode:" +
                std::to_string(errcode));
    }

    verbose("writeDocument(" + std::string(doc->id.buf, doc->id.size) +
            ", db_seq:" + std::to_string(docinfo->db_seq) + ", rev_seq:" +
            std::to_string(docinfo->rev_seq) + ", preserveSeqno:" +
            std::to_string(preserveSeqno) + ")");
}

void OutputCouchFile::setVBState(const std::string& inputVBS,
                                 bool removeFailoverTable) {
    unique_cJSON_ptr json(cJSON_Parse(inputVBS.c_str()));

    if (removeFailoverTable) {
        // If splitting xattrs we really must ensure no rollback can occur
        // Remove the failover_table so we get a fresh vbuuid on restart
        cJSON_DeleteItemFromObject(json.get(), "failover_table");
    }

    unique_cJSON_print_ptr output(cJSON_PrintUnformatted(json.get()));

    writeLocalDocument("_local/vbstate", std::string(output.get()));
}

void OutputCouchFile::writeLocalDocument(const std::string& documentName,
                                         const std::string& value) const {
    LocalDoc localDoc;
    localDoc.id.buf = const_cast<char*>(documentName.c_str());
    localDoc.id.size = documentName.size();
    localDoc.json.buf = const_cast<char*>(value.c_str());
    localDoc.json.size = value.size();
    localDoc.deleted = 0;

    auto errcode = couchstore_save_local_document(db, &localDoc);
    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::writeLocalDocument failed "
                "couchstore_open_local_document documentName:" +
                documentName + " value:" + value + " errcode:" +
                std::to_string(errcode));
    }
    verbose("writeLocalDocument(" + documentName + ", " + value + ") success");
}

void OutputCouchFile::writePartiallyNamespaced() const {
    writeLocalDocument(CouchFile::namespaceName, R"({"namespaces":[]})");
}

void OutputCouchFile::writeCompletleyNamespaced() const {
    std::string json = R"({"namespaces":[)";
    for (auto ns : validNamespaces) {
        json += "{";
        switch (ns) {
        case DocNamespace::DefaultCollection:
            json += R"("name":"DefaultCollection")";
            break;
        case DocNamespace::Collections:
            json += R"("name":"Collections")";
            break;
        case DocNamespace::System:
            json += R"("name":"System")";
            break;
        case DocNamespace::UserXattrs:
            json += R"("name":"UserXattrs")";
            break;
        case DocNamespace::SystemXattrs:
            json += R"("name":"SystemXattrs")";
            break;
        }
        json += R"(,"value":)" + std::to_string(int(ns)) + "},";
    }
    json.pop_back();
    json += "]}";
    writeLocalDocument(CouchFile::namespaceName, json);
}
