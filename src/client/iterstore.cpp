/*
 * Copyright (c) 2016, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Bodies of member functions of the IterStore class.
 * They are pass through functions to the ClientLib class. */

#include <string>

#include "iterstore.hpp"
#include "clientlib.hpp"

using std::string;

IterStore::IterStore(int machine_id, const Config& config) {
  ClientLib::CreateInstance(machine_id, config);
}

IterStore::~IterStore() {
  client_lib->shutdown();
}

void IterStore::ThreadStart() {
  client_lib->thread_start();
}

void IterStore::ThreadStop() {
  client_lib->thread_stop();
}

void IterStore::Update(
    const RowKey& row_key, const RowData *update) {
  client_lib->update_row(row_key, update);
}

void IterStore::Read(
    RowData *buffer, const RowKey& row_key, iter_t slack) {
  client_lib->read_row(buffer, row_key, slack);
}

RowData *IterStore::LocalAccess(const RowKey& row_key) {
  return client_lib->local_access(row_key);
}

void IterStore::Iterate(double progress) {
  client_lib->iterate(progress);
}

void IterStore::VirtualUpdate(const RowKey& row_key) {
  client_lib->virtual_write(row_key);
}

void IterStore::VirtualRead(const RowKey& row_key, iter_t slack) {
  client_lib->virtual_read(row_key, slack);
}

void IterStore::VirtualLocalAccess(const RowKey& row_key) {
  client_lib->virtual_local_access(row_key);
}

void IterStore::VirtualIterate() {
  client_lib->virtual_clock();
}

void IterStore::FinishVirtualIteration() {
  client_lib->finish_virtual_iteration();
}

string IterStore::GetStats() {
  return client_lib->json_stats();
}
