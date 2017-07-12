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

#include "xattr_couchfile.h"

#include <xattr/blob.h>
#include <xattr/utils.h>

XattrCouchFile::XattrCouchFile(OptionsSet options, const std::string& filename)
    : OutputCouchFile(options, filename) {
}

XattrCouchFile::~XattrCouchFile() {
    verbose("~XattrCouchFile: processed " + std::to_string(xattrsProcessed) +
            " xattr documents");
}

void XattrCouchFile::getAllDocs(couchstore_changes_callback_fn callback,
                                void* ctx) const {
    auto errcode =
            couchstore_all_docs(db, nullptr, 0 /*no options*/, callback, ctx);
    if (errcode) {
        throw std::runtime_error(
                "InputCouchFile::upgrade couchstore_all_docs errcode:" +
                std::to_string(errcode));
    }
}

int XattrCouchFile::getNumberProcessed() const {
    return xattrsProcessed;
}

Doc XattrCouchFile::processXattrs(const Doc* doc,
                                  const DocInfo* docinfo) const {
    // Do nothing if we're not splitting xattrs?
    if (!options.test(Options::SplitXattrs)) {
        return *doc;
    }
    xattrsProcessed++;

    cb::xattr::Blob blob(
            {reinterpret_cast<uint8_t*>(doc->data.buf), doc->data.size});

    // split input blob across two new blobs
    cb::xattr::Blob systemXattrs;
    cb::xattr::Blob userXattrs;

    for (auto v : blob) {
        if (cb::xattr::is_system_xattr(v.first)) {
            systemXattrs.set(v.first, v.second);
        } else {
            userXattrs.set(v.first, v.second);
        }
    }

    if (systemXattrs.finalize().size()) {
        Key key(docinfo->id, DocNamespace::SystemXattrs);
        writeXattrs(key, systemXattrs.finalize(), doc, docinfo);
    }

    if (userXattrs.finalize().size()) {
        Key key(docinfo->id, DocNamespace::UserXattrs);
        writeXattrs(key, userXattrs.finalize(), doc, docinfo);
    }

    // Now return Doc without the xattrs
    auto offset = cb::xattr::get_body_offset({doc->data.buf, doc->data.size});
    Doc rv = *doc;
    rv.data.buf += offset;
    rv.data.size -= offset;
    return rv;
}

void XattrCouchFile::writeXattrs(const Key& key,
                                 cb::const_byte_buffer xattrs,
                                 const Doc* doc,
                                 const DocInfo* docinfo) const {
    Doc newDoc = *doc;
    DocInfo newDocInfo = *docinfo;
    newDoc.id.buf = const_cast<char*>(key.data());
    newDoc.id.size = key.size();
    newDocInfo.id = newDoc.id;
    newDoc.data.buf =
            const_cast<char*>(reinterpret_cast<const char*>(xattrs.data()));
    newDoc.data.size = xattrs.size();

    writeDocument(&newDoc, &newDocInfo, false /*preserveSeqno*/);
}

XattrCouchFile::Key::Key(const sized_buf id, DocNamespace ns) {
    key.reserve(id.size + 2);
    key.insert(key.begin(),
               1,
               ns == DocNamespace::UserXattrs
                       ? ':'
                       : '#'); // char(DocNamespace::DefaultCollection));
    key.insert(key.begin(), 1, char(ns));
    std::copy(id.buf, id.buf + id.size, std::back_inserter(key));
}

const char* XattrCouchFile::Key::data() const {
    return key.data();
}

size_t XattrCouchFile::Key::size() const {
    return key.size();
}
