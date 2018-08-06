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

#include <getopt.h>
#include <iostream>
#include <memory>
#include <vector>

#include "input_couchfile.h"
#include "output_couchfile.h"

static bool run(InputCouchFile& input, OutputCouchFile& output) {
    if (!input.preflightChecks()) {
        std::cerr << "Pre-upgrade checks have failed\n";
        return false;
    }

    output.writePartiallyNamespaced();
    output.commit();
    input.upgrade(output);
    output.commit();
    output.writeCompletleyNamespaced();
    output.commit();
    return true;
}

static void usage() {
    std::cout <<
            R"(Usage:
              -v   Optional: Run with verbose output to stdout.
              -i   Required: Input filename.
              -o   Required: Output filename to be created.)"
              << std::endl;
}

struct ProgramOptions {
    OptionsSet options;
    const char* inputFilename;
    const char* outputFilename;
};

static ProgramOptions parseArguments(int argc, char** argv) {
    int cmd = 0;
    ProgramOptions pOptions{};
    while ((cmd = getopt(argc, argv, "i:o:v")) != -1) {
        switch (cmd) {
        case 'v': {
            pOptions.options.set(Options::Verbose);
            std::cout << "Enabling Verbose\n";
            break;
        }
        case 'i': {
            pOptions.inputFilename = optarg;
            std::cout << "Input:" << optarg << "\n";
            break;
        }
        case 'o': {
            pOptions.outputFilename = optarg;
            std::cout << "Output:" << optarg << "\n";
            break;
        }
        case ':':
        case '?': {
            usage();
            throw std::invalid_argument("Invalid Argument");
            break;
        }
        }
    }

    if (pOptions.inputFilename == nullptr) {
        usage();
        throw std::invalid_argument("Missing -i");
    }
    if (pOptions.outputFilename == nullptr) {
        usage();
        throw std::invalid_argument("Missing -o");
    }

    return pOptions;
}

int main(int argc, char** argv) {
    bool success = true;
    try {
        auto options = parseArguments(argc, argv);
        InputCouchFile input(options.options, options.inputFilename);
        OutputCouchFile output(options.options, options.outputFilename);
        success = run(input, output);
    } catch (const std::exception& e) {
        success = false;
        std::cerr << "An exception occurred: " << e.what() << std::endl;
    }

    if (!success) {
        std::cerr << "Terminating with exit code 1\n";
    }

    return success ? 0 : 1;
}