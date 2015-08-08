/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "settings.h"

/**
 * Initialize breakpad based on the specified settings struct.
 *
 * The function may be called multiple times and allow for reconfiguration
 * of the breakpad settings.
 */
void initialize_breakpad(const breakpad_settings_t* settings);

/**
 * Cleaning up when breakpad no longer needed
 * (Assuming it is enabled and has been initialized)
 */
void destroy_breakpad(void);
