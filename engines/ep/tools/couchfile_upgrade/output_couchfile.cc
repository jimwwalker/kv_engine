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

#include <cJSON.h>
#include <cJSON_utils.h>

OutputCouchFile::OutputCouchFile(OptionsSet options,
                                 const std::string& filename)
    : CouchFile(options, filename, COUCHSTORE_OPEN_FLAG_CREATE) {
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
        const sized_buf in, CollectionID newNamespace) const {
    std::vector<char> rv(in.size + sizeof(CollectionID));
    *reinterpret_cast<CollectionID*>(&rv[0]) = newNamespace;
    std::copy(in.buf, in.buf + in.size, rv.begin() + sizeof(CollectionID));
    return rv;
}

void OutputCouchFile::processDocument(const Doc* doc,
                                      const DocInfo* docinfo) const {
    auto newName =
            createNamespacedName(doc->id, CollectionID::DefaultCollection);
    Doc newDoc = *doc;
    DocInfo newDocInfo = *docinfo;
    newDoc.id.buf = newName.data();
    newDoc.id.size = newName.size();
    newDocInfo.id = newDoc.id;

    writeDocument(&newDoc, &newDocInfo);
}

void OutputCouchFile::writeDocument(const Doc* doc,
                                    const DocInfo* docinfo) const {
    auto errcode = couchstore_save_document(
            db,
            doc,
            const_cast<DocInfo*>(docinfo),
            COMPRESS_DOC_BODIES | COUCHSTORE_SEQUENCE_AS_IS);

    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::writeDocument couchstore_save_document "
                "errcode:" +
                std::to_string(errcode));
    }

    verbose("writeDocument(" + std::string(doc->id.buf, doc->id.size) +
            ", db_seq:" + std::to_string(docinfo->db_seq) +
            ", rev_seq:" + std::to_string(docinfo->rev_seq));
}

void OutputCouchFile::setVBState(const std::string& inputVBS) {
    writeLocalDocument("_local/vbstate", inputVBS);
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
                documentName + " value:" + value +
                " errcode:" + std::to_string(errcode));
    }
    verbose("writeLocalDocument(" + documentName + ", " + value + ") success");
}

void OutputCouchFile::writePartiallyNamespaced() const {
    writeLocalDocument(CouchFile::namespaceName, R"({"namespaced":false})");
}

void OutputCouchFile::writeCompletleyNamespaced() const {
    writeLocalDocument(CouchFile::namespaceName, R"({"namespaced":true})");
}
