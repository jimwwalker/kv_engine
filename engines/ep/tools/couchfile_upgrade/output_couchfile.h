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

#pragma once

#include "couchfile.h"

#include <include/memcached/dockey.h>

class XattrCouchFile;
class OutputCouchFile : public CouchFile {
public:
    OutputCouchFile(OptionsSet options, const std::string& filename);

    void commit() const;

    std::vector<char> createNamespacedName(const sized_buf in,
                                           DocNamespace newNamespace) const;

    void merge(const XattrCouchFile& input);

    void processDocument(const Doc* doc,
                         const DocInfo* docinfo,
                         bool preserveSeqno) const;

    void setVBState(const std::string& inputVBS, bool removeFailoverTable);

    void writePartiallyNamespaced() const;

    void writeCompletleyNamespaced() const;

    void writeDocument(const Doc* doc,
                       const DocInfo* docinfo,
                       bool preserveSeqno) const;

protected:
    void writeLocalDocument(const std::string& documentName,
                            const std::string& value) const;

    std::vector<DocNamespace> validNamespaces;
};