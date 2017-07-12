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

#include "output_couchfile.h"

class XattrCouchFile : public OutputCouchFile {
public:
    XattrCouchFile(OptionsSet options, const std::string& filename);
    ~XattrCouchFile();

    void getAllDocs(couchstore_changes_callback_fn callback, void* ctx) const;

    int getNumberProcessed() const;

    Doc processXattrs(const Doc* doc, const DocInfo* docinfo) const;

private:
    struct Key {
        Key(const sized_buf id, DocNamespace ns);

        const char* data() const;

        size_t size() const;

        std::vector<char> key;
    };

    void writeXattrs(const Key& key,
                     cb::const_byte_buffer xattrs,
                     const Doc* doc,
                     const DocInfo* docinfo) const;

    mutable int xattrsProcessed = 0;
};