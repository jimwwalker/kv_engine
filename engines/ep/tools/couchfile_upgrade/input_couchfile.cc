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

#include "input_couchfile.h"
#include "couch-kvstore/couch-kvstore-metadata.h"
#include "output_couchfile.h"

InputCouchFile::InputCouchFile(OptionsSet options, const std::string& filename)
    : CouchFile(options, filename, 0) {
}

/**
 * @return true if the checks pass
 */
bool InputCouchFile::preflightChecks() const {
    if (isNotNamespaced()) {
        return true;
    }

    bool partial = isPartiallyNamespaced();
    bool complete = isCompletelyNamespaced();

    if (partial && complete) {
        std::cerr << "Error: filename:" << filename
                  << " is showing both partial and complete\n";
    }

    if (partial || complete) {
        std::string error = partial ? "partially processed" : "processed";
        std::cerr << "Error: filename:" << filename << " is already " << error
                  << "\n";
    }

    return !(partial || complete);
}

struct UpgradeCouchFileContext {
    OutputCouchFile& output;
};

static int upgradeCallback(Db* db, DocInfo* docinfo, void* ctx) {
    DocPtr doc;
    auto errcode = couchstore_open_doc_with_docinfo(
            db, docinfo, doc.getDocAddress(), DECOMPRESS_DOC_BODIES);
    if (errcode) {
        throw std::runtime_error(
                "InputCouchFile::upgradeCallback "
                "couchstore_open_doc_with_docinfo errcode:" +
                std::to_string(errcode));
    } else {
        auto documentMetaData =
                MetaDataFactory::createMetaData(docinfo->rev_meta);

        if (documentMetaData->getVersionInitialisedFrom() ==
            MetaData::Version::V0) {
            // Maybe we should do this....
            throw std::runtime_error(
                    "OutputCouchFile::writeDocument cannot process "
                    "documents with "
                    "V0 meta");
        }

        const auto* context = reinterpret_cast<UpgradeCouchFileContext*>(ctx);
        context->output.processDocument(doc.getDoc(), docinfo);
    }
    return 0;
}

void InputCouchFile::upgrade(OutputCouchFile& output) const {
    UpgradeCouchFileContext context{output};

    auto errcode = couchstore_all_docs(
            db, nullptr, 0 /*no options*/, &upgradeCallback, &context);
    if (errcode) {
        throw std::runtime_error(
                "InputCouchFile::upgrade couchstore_all_docs errcode:" +
                std::to_string(errcode));
    }

    // Finally read, adjust and write _local/vbstate
    auto vbstate = getLocalDocument("_local/vbstate");
    output.setVBState(vbstate);
    output.commit();
}

bool InputCouchFile::doesLocalDocumentExist(
        const std::string& documentName) const {
    return openLocalDocument(documentName) != nullptr;
}

std::string InputCouchFile::getLocalDocument(
        const std::string& documentName) const {
    auto result = openLocalDocument(documentName);
    if (!result) {
        throw std::runtime_error(
                "InputCouchFile::getLocalDocument openLocalDocument(" +
                documentName + ") failed");
    }

    verbose("getLocalDocument(" + documentName + ")");
    return std::string(result->json.buf, result->json.size);
}

bool InputCouchFile::isCompletelyNamespaced() const {
    return getLocalDocument(CouchFile::namespaceName) == "complete";
}

bool InputCouchFile::isNotNamespaced() const {
    return !doesLocalDocumentExist(CouchFile::namespaceName);
}

bool InputCouchFile::isPartiallyNamespaced() const {
    return getLocalDocument(CouchFile::namespaceName) == "partial";
}

LocalDocPtr InputCouchFile::openLocalDocument(
        const std::string& documentName) const {
    sized_buf id;
    id.buf = const_cast<char*>(documentName.c_str());
    id.size = documentName.size();
    LocalDoc* localDoc = nullptr;
    auto errcode =
            couchstore_open_local_document(db, id.buf, id.size, &localDoc);

    switch (errcode) {
    case COUCHSTORE_SUCCESS:
        return LocalDocPtr(localDoc);
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        return {};
    default:
        throw std::runtime_error("InputCouchFile::localDocumentExists(" +
                                 documentName +
                                 ") error:" + std::to_string(errcode));
    }

    return {};
}
