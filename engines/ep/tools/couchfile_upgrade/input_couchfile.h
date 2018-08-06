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

class OutputCouchFile;
class InputCouchFile : public CouchFile {
public:
    InputCouchFile(OptionsSet options, const std::string& filename);

    /**
     * Perform some checks on the input file.
     * Check it's not already marked as namespaced (or partially processed)
     */
    bool preflightChecks() const;

    /**
     * Upgrade this input couchfile and send the new file to output couchfile
     */
    void upgrade(OutputCouchFile& output) const;

private:
    bool doesLocalDocumentExist(const std::string& documentName) const;

    std::string getLocalDocument(const std::string& documentName) const;

    bool isCompletelyNamespaced() const;

    bool isNotNamespaced() const;

    bool isPartiallyNamespaced() const;

    LocalDocPtr openLocalDocument(const std::string& documentName) const;
};