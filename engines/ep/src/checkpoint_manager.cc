/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "checkpoint_manager.h"

#include "bucket_logger.h"
#include "callbacks.h"
#include "checkpoint.h"
#include "checkpoint_config.h"
#include "ep_time.h"
#include "pre_link_document_context.h"
#include "stats.h"
#include "statwriter.h"
#include "vbucket.h"

#include <gsl.h>

CheckpointManager::CheckpointManager(EPStats& st,
                                     Vbid vbucket,
                                     CheckpointConfig& config,
                                     int64_t lastSeqno,
                                     uint64_t lastSnapStart,
                                     uint64_t lastSnapEnd,
                                     FlusherCallback cb)
    : stats(st),
      checkpointConfig(config),
      vbucketId(vbucket),
      numItems(0),
      lastBySeqno(lastSeqno),
      pCursorPreCheckpointId(0),
      flusherCB(cb) {
    LockHolder lh(queueLock);

    lastBySeqno.setLabel("CheckpointManager(" + vbucketId.to_string() +
                         ")::lastBySeqno");

    // Note: this is the last moment in the CheckpointManager lifetime
    //     when the checkpointList is empty.
    //     Only in CheckpointManager::clear_UNLOCKED, the checkpointList
    //     is temporarily cleared and a new open checkpoint added immediately.
    addOpenCheckpoint(1, lastSnapStart, lastSnapEnd, CheckpointType::Memory);

    if (checkpointConfig.isPersistenceEnabled()) {
        // Register the persistence cursor
        pCursor = registerCursorBySeqno_UNLOCKED(lh, pCursorName, lastBySeqno)
                          .cursor;
        persistenceCursor = pCursor.lock().get();
    }
}

uint64_t CheckpointManager::getOpenCheckpointId_UNLOCKED(const LockHolder& lh) {
    return getOpenCheckpoint_UNLOCKED(lh).getId();
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    LockHolder lh(queueLock);
    return getOpenCheckpointId_UNLOCKED(lh);
}

uint64_t CheckpointManager::getLastClosedCheckpointId_UNLOCKED(
        const LockHolder& lh) {
    auto id = getOpenCheckpointId_UNLOCKED(lh);
    return id > 0 ? (id - 1) : 0;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() {
    LockHolder lh(queueLock);
    return getLastClosedCheckpointId_UNLOCKED(lh);
}

void CheckpointManager::setOpenCheckpointId(uint64_t id) {
    LockHolder lh(queueLock);
    setOpenCheckpointId_UNLOCKED(lh, id);
}

void CheckpointManager::setOpenCheckpointId_UNLOCKED(const LockHolder& lh,
                                                     uint64_t id) {
    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // Update the checkpoint_start item with the new Id.
    const auto ckpt_start = ++(openCkpt.begin());
    (*ckpt_start)->setRevSeqno(id);
    if (openCkpt.getId() == 0) {
        (*ckpt_start)->setBySeqno(lastBySeqno + 1);
        openCkpt.setSnapshotStartSeqno(lastBySeqno);
        openCkpt.setSnapshotEndSeqno(lastBySeqno);
    }

    // Update any set_vbstate items to have the same seqno as the
    // checkpoint_start.
    const auto ckpt_start_seqno = (*ckpt_start)->getBySeqno();
    for (auto item = std::next(ckpt_start); item != openCkpt.end(); item++) {
        if ((*item)->getOperation() == queue_op::set_vbucket_state) {
            (*item)->setBySeqno(ckpt_start_seqno);
        }
    }

    openCkpt.setId(id);
    EP_LOG_DEBUG(
            "Set the current open checkpoint id to {} for {} bySeqno is "
            "{}, max is {}",
            id,
            vbucketId,
            (*ckpt_start)->getBySeqno(),
            lastBySeqno);
}

Checkpoint& CheckpointManager::getOpenCheckpoint_UNLOCKED(
        const LockHolder&) const {
    // During its lifetime, the checkpointList can only be in one of the
    // following states:
    //
    //     - 1 open checkpoint, after the execution of:
    //         - CheckpointManager::CheckpointManager
    //         - CheckpointManager::clear_UNLOCKED
    //     - [1, N] closed checkpoints + 1 open checkpoint, after the execution
    //         of CheckpointManager::closeOpenCheckpointAndAddNew_UNLOCKED
    //
    // Thus, by definition checkpointList.back() is the open checkpoint and the
    // checkpointList is never empty.
    return *checkpointList.back();
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    addNewCheckpoint_UNLOCKED(
            id, lastBySeqno, lastBySeqno, CheckpointType::Memory);
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(
        uint64_t id,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        CheckpointType checkpointType) {
    // First, we must close the open checkpoint.
    auto& oldOpenCkpt = *checkpointList.back();
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint_UNLOCKED: Close "
            "the current open checkpoint: [{}, id:{}, snapStart:{}, "
            "snapEnd:{}]",
            vbucketId,
            oldOpenCkpt.getId(),
            oldOpenCkpt.getLowSeqno(),
            oldOpenCkpt.getHighSeqno());
    queued_item qi = createCheckpointItem(
            oldOpenCkpt.getId(), vbucketId, queue_op::checkpoint_end);
    oldOpenCkpt.queueDirty(qi, this);
    ++numItems;
    oldOpenCkpt.setState(CHECKPOINT_CLOSED);

    // Now, we can create the new open checkpoint
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint_UNLOCKED: Create "
            "a new open checkpoint: [{}, id:{}, snapStart:{}, snapEnd:{}]",
            vbucketId,
            id,
            snapStartSeqno,
            snapEndSeqno);
    addOpenCheckpoint(id, snapStartSeqno, snapEndSeqno, checkpointType);

    /* If cursors reached to the end of its current checkpoint, move it to the
       next checkpoint. DCP and Persistence cursors can skip a "checkpoint end"
       meta item. This is needed so that the checkpoint remover can remove the
       closed checkpoints and hence reduce the memory usage */
    for (auto& cur_it : connCursors) {
        CheckpointCursor& cursor = *cur_it.second;
        ++(cursor.currentPos);
        if (cursor.currentPos != (*(cursor.currentCheckpoint))->end() &&
            (*(cursor.currentPos))->getOperation() ==
                    queue_op::checkpoint_end) {
            /* checkpoint_end meta item is skipped for persistence and
             * DCP cursors */
            ++(cursor.currentPos); // cursor now reaches to the checkpoint end
        }

        if (cursor.currentPos == (*(cursor.currentCheckpoint))->end()) {
           if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
               if (!moveCursorToNextCheckpoint(cursor)) {
                   --(cursor.currentPos);
               }
           } else {
               // The replication cursor is already reached to the end of
               // the open checkpoint.
               --(cursor.currentPos);
           }
        } else {
            --(cursor.currentPos);
        }
    }
}

void CheckpointManager::addOpenCheckpoint(uint64_t id,
                                          uint64_t snapStart,
                                          uint64_t snapEnd,
                                          CheckpointType checkpointType) {
    Expects(checkpointList.empty() ||
            checkpointList.back()->getState() ==
                    checkpoint_state::CHECKPOINT_CLOSED);

    auto ckpt = std::make_unique<Checkpoint>(
            stats, id, snapStart, snapEnd, vbucketId, checkpointType);
    // Add an empty-item into the new checkpoint.
    // We need this because every CheckpointCursor will point to this empty-item
    // at creation. So, the cursor will point at the first actual non-meta item
    // after the first cursor-increment.
    queued_item qi = createCheckpointItem(0, Vbid(0xffff), queue_op::empty);
    ckpt->queueDirty(qi, this);
    // Note: We don't include the empty-item in 'numItems'

    // This item represents the start of the new checkpoint
    qi = createCheckpointItem(id, vbucketId, queue_op::checkpoint_start);
    ckpt->queueDirty(qi, this);
    ++numItems;

    checkpointList.push_back(std::move(ckpt));
    Ensures(!checkpointList.empty());
    Ensures(checkpointList.back()->getState() ==
            checkpoint_state::CHECKPOINT_OPEN);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
        const std::string& name, uint64_t startBySeqno) {
    LockHolder lh(queueLock);
    return registerCursorBySeqno_UNLOCKED(lh, name, startBySeqno);
}

CursorRegResult CheckpointManager::registerCursorBySeqno_UNLOCKED(
        const LockHolder& lh, const std::string& name, uint64_t startBySeqno) {
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    if (openCkpt.getHighSeqno() < startBySeqno) {
        throw std::invalid_argument(
                "CheckpointManager::registerCursorBySeqno:"
                " startBySeqno (which is " +
                std::to_string(startBySeqno) +
                ") is less than last "
                "checkpoint highSeqno (which is " +
                std::to_string(openCkpt.getHighSeqno()) + ")");
    }

    // If cursor exists with the same name as the one being created, then
    // remove it.
    for (const auto& cursor : connCursors) {
        if (cursor.first == name) {
            removeCursor_UNLOCKED(cursor.second.get());
            break;
        }
    }

    CursorRegResult result;
    result.seqno = std::numeric_limits<uint64_t>::max();
    result.tryBackfill = false;

    auto itr = checkpointList.begin();
    for (; itr != checkpointList.end(); ++itr) {
        uint64_t en = (*itr)->getHighSeqno();
        uint64_t st = (*itr)->getLowSeqno();

        if (startBySeqno < st) {
            // Requested sequence number is before the start of this
            // checkpoint, position cursor at the checkpoint start.
            auto cursor = std::make_shared<CheckpointCursor>(name,
                                                             itr,
                                                             (*itr)->begin());
            connCursors[name] = cursor;
            (*itr)->incNumOfCursorsInCheckpoint();
            result.seqno = st;
            result.cursor.setCursor(cursor);
            result.tryBackfill = true;
            break;
        } else if (startBySeqno <= en) {
            // Requested sequence number lies within this checkpoint.
            // Calculate which item to position the cursor at.
            ChkptQueueIterator iitr = (*itr)->begin();
            while (++iitr != (*itr)->end() &&
                    (startBySeqno >=
                     static_cast<uint64_t>((*iitr)->getBySeqno()))) {
            }

            if (iitr == (*itr)->end()) {
                --iitr;
                result.seqno = static_cast<uint64_t>((*iitr)->getBySeqno()) + 1;
            } else {
                result.seqno = static_cast<uint64_t>((*iitr)->getBySeqno());
                --iitr;
            }

            auto cursor =
                    std::make_shared<CheckpointCursor>(name, itr, iitr);
            connCursors[name] = cursor;
            (*itr)->incNumOfCursorsInCheckpoint();
            result.cursor.setCursor(cursor);
            break;
        }
    }

    if (result.seqno == std::numeric_limits<uint64_t>::max()) {
        /*
         * We should never get here since this would mean that the sequence
         * number we are looking for is higher than anything currently assigned
         *  and there is already an assert above for this case.
         */
        throw std::logic_error(
                "CheckpointManager::registerCursorBySeqno the sequences number "
                "is higher than anything currently assigned");
    }
    return result;
}

bool CheckpointManager::removeCursor(const CheckpointCursor* cursor) {
    LockHolder lh(queueLock);
    return removeCursor_UNLOCKED(cursor);
}

bool CheckpointManager::removeCursor_UNLOCKED(const CheckpointCursor* cursor) {
    if (!cursor) {
        return false;
    }

    EP_LOG_DEBUG("Remove the checkpoint cursor with the name \"{}\" from {}",
                 cursor->name,
                 vbucketId);

    (*cursor->currentCheckpoint)->decNumOfCursorsInCheckpoint();

    if (connCursors.erase(cursor->name) == 0) {
        throw std::logic_error(
                "CheckpointManager::removeCursor_UNLOCKED failed to remove "
                "name:" +
                cursor->name);
    }
    return true;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage_UNLOCKED(
        const LockHolder& lh, const VBucket& vbucket) {
    bool forceCreation = false;
    double memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // pesistence and conn cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
            (connCursors.size() + 1) == openCkpt.getNumCursorsInCheckpoint();

    if (memoryUsed > stats.mem_high_wat && allCursorsInOpenCheckpoint &&
        (openCkpt.getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         openCkpt.getNumItems() == vbucket.ht.getNumInMemoryItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

size_t CheckpointManager::removeClosedUnrefCheckpoints(
        VBucket& vbucket, bool& newOpenCheckpointCreated, size_t limit) {
    // This function is executed periodically by the non-IO dispatcher.
    size_t numUnrefItems = 0;
    // Checkpoints in the `unrefCheckpointList` have to be deleted before we
    // return from this function. With smart pointers, deletion happens when
    // `unrefCheckpointList` goes out of scope (i.e., when this function
    // returns).
    CheckpointList unrefCheckpointList;
    {
        LockHolder lh(queueLock);
        uint64_t oldCheckpointId = 0;
        bool canCreateNewCheckpoint = false;
        if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
            (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
             checkpointList.front()->isNoCursorsInCheckpoint())) {
            canCreateNewCheckpoint = true;
        }
        if (vbucket.getState() == vbucket_state_active &&
            canCreateNewCheckpoint) {
            bool forceCreation =
                    isCheckpointCreationForHighMemUsage_UNLOCKED(lh, vbucket);
            // Check if this master active vbucket needs to create a new open
            // checkpoint.
            oldCheckpointId =
                    checkOpenCheckpoint_UNLOCKED(lh, forceCreation, true);
        }
        newOpenCheckpointCreated = oldCheckpointId > 0;

        if (checkpointConfig.canKeepClosedCheckpoints()) {
            double memoryUsed =
                    static_cast<double>(stats.getEstimatedTotalMemoryUsed());
            if (memoryUsed < stats.mem_high_wat &&
                checkpointList.size() <= checkpointConfig.getMaxCheckpoints()) {
                return 0;
            }
        }

        size_t numMetaItems = 0;
        size_t numCheckpointsRemoved = 0;
        // Iterate through the current checkpoints (from oldest to newest),
        // checking if the checkpoint can be removed.
        // `it` is set to the first checkpoint we want to keep - all earlier
        // ones are removed.
        auto it = checkpointList.begin();
        // Note terminating condition - we stop at one before the last
        // checkpoint - we must leave at least one checkpoint in existence.
        for (; it != checkpointList.end() &&
               std::next(it) != checkpointList.end();
             ++it) {

            // When we encounter the first checkpoint which has cursor(s) in it,
            // or if the persistence cursor is still operating, stop.
            if ((*it)->getNumCursorsInCheckpoint() > 0 ||
                (checkpointConfig.isPersistenceEnabled() &&
                 (*it)->getId() > pCursorPreCheckpointId)) {
                break;
            } else {
                numUnrefItems += (*it)->getNumItems();
                numMetaItems += (*it)->getNumMetaItems();
                ++numCheckpointsRemoved;

                if (numCheckpointsRemoved >= limit) {
                    ++it;
                    break;
                }

                if (checkpointConfig.canKeepClosedCheckpoints() &&
                    (checkpointList.size() - numCheckpointsRemoved) <=
                            checkpointConfig.getMaxCheckpoints()) {
                    // Collect unreferenced closed checkpoints until the number
                    // of checkpoints is
                    // equal to the number of max checkpoints allowed.
                    ++it;
                    break;
                }
            }
        }
        size_t total_items = numUnrefItems + numMetaItems;
        numItems.fetch_sub(total_items);
        unrefCheckpointList.splice(unrefCheckpointList.begin(),
                                   checkpointList,
                                   checkpointList.begin(),
                                   it);
    }
    // Here we have released the lock and unrefCheckpointList is not yet
    // out-of-scope (it will be when this function returns).
    // Thus, checkpoint memory freeing doen't happen under lock.
    // That is very important as releasing objects is an expensive operation, so
    // it would have a relevant impact on front-end operations.
    // Also note that this function is O(N), with N being checkpointList.size().

    return numUnrefItems;
}

CheckpointManager::ExpelResult
CheckpointManager::expelUnreferencedCheckpointItems() {
    CheckpointQueue expelledItems;
    {
        LockHolder lh(queueLock);

        const auto containsCursors = [](std::unique_ptr<Checkpoint>& c) {
            if (c->getNumCursorsInCheckpoint() > 0) {
                return true;
            }
            return false;
        };
        // Find the oldest checkpoint with cursors in it.
        const auto it = std::find_if(
                checkpointList.begin(), checkpointList.end(), containsCursors);
        Checkpoint* currentCheckpoint =
                (it == checkpointList.end()) ? nullptr : (*it).get();

        if (currentCheckpoint == nullptr) {
            // There are no eligible checkpoints to expel items from.
            return {};
        }

        if (currentCheckpoint->getNumItems() == 0) {
            // There are no mutation items in the checkpoint to expel.
            return {};
        }

        const auto findLowestCursor =
                [currentCheckpoint](Cursor& current,
                                    const cursor_index::value_type& cursor) {
                    // Get the cursor's current checkpoint.
                    const auto checkpoint =
                            (*(cursor.second->currentCheckpoint)).get();
                    // Is the cursor in the checkpoint we are interested in?
                    if (currentCheckpoint != checkpoint) {
                        return current;
                    }
                    // We are in the same checkpoint, have we found any
                    // previous cursors?
                    auto currentCursor = current.lock();
                    if (currentCursor == nullptr) {
                        // No, so this has got to be the lowest we currently
                        // know about.
                        current.setCursor(cursor.second);
                        return current;
                    }
                    // We already have a cursor so need to see if this one
                    // is lower. Get the new cursor's seqno.
                    const auto seqno =
                            (*cursor.second->currentPos)->getBySeqno();
                    // Does it have a seqno lower than our current lowest?
                    const auto currentLowestSeqno =
                            (*currentCursor->currentPos)->getBySeqno();
                    if (seqno < currentLowestSeqno) {
                        // Yes, so make it the new lowest.
                        current.setCursor(cursor.second);
                    }
                    return current;
                };

        Cursor lowestCursor;
        // Find the cursor with the lowest seqno that resides in the
        // currentCheckpoint.
        lowestCursor = std::accumulate(connCursors.begin(),
                                       connCursors.end(),
                                       lowestCursor,
                                       findLowestCursor);

        // Create a cursor that marks where we will expel upto and
        // including.
        auto lowestCheckpointCursor = lowestCursor.lock();
        if (lowestCheckpointCursor == nullptr) {
            // Failed to get a shared_ptr to the lowest checkpoint cursor
            // so just return.
            return {};
        }

        auto expelUpToAndIncluding = CheckpointCursor(
                "expelUpToAndIncluding",
                lowestCheckpointCursor.get()->currentCheckpoint,
                lowestCheckpointCursor.get()->currentPos);

        auto& iterator = (expelUpToAndIncluding.currentPos);

        /*
         * Walk backwards over the checkpoint if not yet reached the dummy item,
         * and pointing to an item that either:
         * 1. has a seqno equal to the checkpoint's high seqno, or
         * 2. has a previous entry with the same seqno, or
         * 3. is pointing to a metadata item.
         */
        while ((iterator != currentCheckpoint->begin()) &&
               (((*iterator)->getBySeqno() ==
                 int64_t(currentCheckpoint->getHighSeqno())) ||
                ((*std::prev(iterator))->getBySeqno() ==
                 (*iterator)->getBySeqno()) ||
                ((*iterator)->isCheckPointMetaItem()))) {
            --iterator;
        }

        // If pointing to the dummy item then cannot expel anything and so just
        // return.
        if (iterator == currentCheckpoint->begin()) {
            return {};
        }

        /*
         * Now have the checkpoint and the expelUpToAndIncluding
         * cursor we can expel the relevant checkpoint items.  The
         * method returns the expelled items in the expelledItems
         * queue thereby ensuring they still have a reference whilst
         * the queuelock is being held.
         */
        expelledItems = currentCheckpoint->expelItems(expelUpToAndIncluding);
    }

    // If called currentCheckpoint->expelItems but did not manage to expel
    // anything then just return.
    if (expelledItems.empty()) {
        return {};
    }

    stats.itemsExpelledFromCheckpoints.fetch_add(expelledItems.size());

    /*
     * Calculate an *estimate* of the amount of memory we will recover.
     * This is comprised of two parts:
     * 1. Memory used by each item to be expelled.  For each item this
     *    is calculated as the sizeof(Item) + key size + value size.
     * 2. Memory used to hold the items in the checkpoint list.
     *    The checkpoint list will be shorter by expelledItems.size().
     *    This saving is equal to the memory allocated by the
     *    expelledItems list.  Note: On Windows this is not strictly
     *    true as we allocate space for size + 1.  However as its
     *    an estimate we do not need to adjust.
     *
     * It is an optimistic estimate as it assumes that each queued_item
     * is not referenced by anyone else (e.g. a DCP stream) and therefore
     * its reference count will drop to zero on exiting the function
     * allowing the memory to be freed.
     */

    // Part 1 of calculating the estimate (see comment above).
    size_t estimateOfAmountOfRecoveredMemory{0};
    for (const auto& ei : expelledItems) {
        estimateOfAmountOfRecoveredMemory += ei->size();
    }

    // Part 2 of calculating the estimate (see comment above).
    estimateOfAmountOfRecoveredMemory +=
            *(expelledItems.get_allocator().getBytesAllocated());

    /*
     * We are now outside of the queueLock when the method exits,
     * expelledItems will go out of scope and so the reference count
     * of expelled items will go to zero and hence will be deleted
     * outside of the queuelock.
     */
    return {expelledItems.size(), estimateOfAmountOfRecoveredMemory};
}

std::vector<Cursor> CheckpointManager::getListOfCursorsToDrop() {
    LockHolder lh(queueLock);

    Checkpoint* persistentCheckpoint =
            (persistenceCursor == nullptr)
                    ? nullptr
                    : persistenceCursor->currentCheckpoint->get();
    /*
     * Iterate through the list of checkpoints and add the checkpoint to
     * a set of valid checkpoints until we reach either an open checkpoint
     * or a checkpoint that contains the persistence cursor.
     */
    std::unordered_set<Checkpoint*> validChkpts;
    for (const auto& chkpt : checkpointList) {
        if (persistentCheckpoint == chkpt.get() ||
            chkpt->getState() == CHECKPOINT_OPEN) {
            break;
        } else {
            validChkpts.insert(chkpt.get());
        }
    }

    /*
     * If we cannot find any valid checkpoints to remove cursors from
     * then just return an empty vector.
     */
    if (validChkpts.empty()) {
        return {};
    }

    /*
     * Iterate through all cursors and if the cursor resides in one of the
     * valid checkpoints (i.e. a checkpoint that cursors can be deleted
     * from) then add the cursor to the cursorsToDrop vector.
     */
    std::vector<Cursor> cursorsToDrop;
    for (const auto& cursor : connCursors) {
        if (validChkpts.count(cursor.second->currentCheckpoint->get()) > 0) {
            cursorsToDrop.emplace_back(cursor.second);
        }
    }
    return cursorsToDrop;
}

bool CheckpointManager::hasClosedCheckpointWhichCanBeRemoved() const {
    LockHolder lh(queueLock);
    // Check oldest checkpoint; if closed and contains no cursors then
    // we can remove it (and possibly additional old-but-not-oldest
    // checkpoints).
    const auto& oldestCkpt = checkpointList.front();
    return (oldestCkpt->getState() == CHECKPOINT_CLOSED) &&
           (oldestCkpt->isNoCursorsInCheckpoint());
}

void CheckpointManager::updateStatsForNewQueuedItem_UNLOCKED(
        const LockHolder& lh, VBucket& vb, const queued_item& qi) {
    ++stats.totalEnqueued;
    if (checkpointConfig.isPersistenceEnabled()) {
        ++stats.diskQueueSize;
        vb.doStatsForQueueing(*qi, qi->size());
    }
}

bool CheckpointManager::queueDirty(
        VBucket& vb,
        queued_item& qi,
        const GenerateBySeqno generateBySeqno,
        const GenerateCas generateCas,
        PreLinkDocumentContext* preLinkDocumentContext) {
    LockHolder lh(queueLock);

    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->isNoCursorsInCheckpoint())) {
        canCreateNewCheckpoint = true;
    }

    if (vb.getState() == vbucket_state_active && canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint_UNLOCKED(lh, false, true);
    }

    auto* openCkpt = &getOpenCheckpoint_UNLOCKED(lh);

    if (GenerateBySeqno::Yes == generateBySeqno) {
        qi->setBySeqno(lastBySeqno + 1);
    }
    const auto newLastBySeqno = qi->getBySeqno();

    // MB-20798: Allow the HLC to be created 'atomically' with the seqno as
    // we're holding the ::queueLock.
    if (GenerateCas::Yes == generateCas) {
        auto cas = vb.nextHLCCas();
        qi->setCas(cas);
        if (preLinkDocumentContext != nullptr) {
            preLinkDocumentContext->preLink(cas, newLastBySeqno);
        }
    }

    QueueDirtyStatus result = openCkpt->queueDirty(qi, this);

    if (result == QueueDirtyStatus::FailureDuplicateItem) {
        // Could not queue into the current checkpoint as it already has a
        // duplicate item (and not permitted to de-dupe this item).
        if (vb.getState() != vbucket_state_active) {
            // We shouldn't see this for non-active vBuckets; given the
            // original (active) vBucket on some other node should not have
            // put duplicate mutations in the same Checkpoint.
            throw std::logic_error(
                    "CheckpointManager::queueDirty(" + vbucketId.to_string() +
                    ") - got Ckpt::queueDirty() status:" + to_string(result) +
                    " when vbstate is non-active:" +
                    std::to_string(vb.getState()));
        }

        // To process this item, create a new (empty) checkpoint which we can
        // then re-attempt the enqueuing.
        // Note this uses the lastBySeqno for snapStart / End.
        checkOpenCheckpoint_UNLOCKED(lh, /*force*/ true, false);
        openCkpt = &getOpenCheckpoint_UNLOCKED(lh);
        result = openCkpt->queueDirty(qi, this);
        if (result != QueueDirtyStatus::SuccessNewItem) {
            throw std::logic_error(
                    "CheckpointManager::queueDirty(vb:" +
                    vbucketId.to_string() +
                    ") - got Ckpt::queueDirty() status:" + to_string(result) +
                    " even after creating a new Checkpoint.");
        }
    }

    lastBySeqno = newLastBySeqno;
    if (GenerateBySeqno::Yes == generateBySeqno) {
        // Now the item has been queued, update snapshotEndSeqno.
        openCkpt->setSnapshotEndSeqno(lastBySeqno);
    }

    // Sanity check that the last seqno is within the open Checkpoint extent.
    auto snapStart = openCkpt->getSnapshotStartSeqno();
    auto snapEnd = openCkpt->getSnapshotEndSeqno();
    if (!(snapStart <= static_cast<uint64_t>(lastBySeqno) &&
          static_cast<uint64_t>(lastBySeqno) <= snapEnd)) {
        throw std::logic_error(
                "CheckpointManager::queueDirty: lastBySeqno "
                "not in snapshot range. " +
                vb.getId().to_string() +
                " state:" + std::string(VBucket::toString(vb.getState())) +
                " snapshotStart:" + std::to_string(snapStart) +
                " lastBySeqno:" + std::to_string(lastBySeqno) +
                " snapshotEnd:" + std::to_string(snapEnd) + " genSeqno:" +
                to_string(generateBySeqno) + " checkpointList.size():" +
                std::to_string(checkpointList.size()));
    }

    switch (result) {
    case QueueDirtyStatus::SuccessExistingItem:
        ++stats.totalDeduplicated;
        return false;
    case QueueDirtyStatus::SuccessNewItem:
        ++numItems;
        // FALLTHROUGH
    case QueueDirtyStatus::SuccessPersistAgain:
        updateStatsForNewQueuedItem_UNLOCKED(lh, vb, qi);
        return true;
    case QueueDirtyStatus::FailureDuplicateItem:
        throw std::logic_error(
                "CheckpointManager::queueDirty: Got invalid "
                "result:FailureDuplicateItem - should have been handled with "
                "retry.");
    }
    throw std::logic_error("queueDirty: Invalid value for QueueDirtyStatus: " +
                           std::to_string(int(result)));
}

void CheckpointManager::queueSetVBState(VBucket& vb) {
    // Take lock to serialize use of {lastBySeqno} and to queue op.
    LockHolder lh(queueLock);

    // Create the setVBState operation, and enqueue it.
    queued_item item = createCheckpointItem(/*id*/0, vbucketId,
                                            queue_op::set_vbucket_state);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto result = openCkpt.queueDirty(item, this);

    if (result == QueueDirtyStatus::SuccessNewItem) {
        ++numItems;
        updateStatsForNewQueuedItem_UNLOCKED(lh, vb, item);
    } else {
        throw std::logic_error(
                "CheckpointManager::queueSetVBState: "
                "expected: SuccessNewItem, got:" +
                to_string(result) + "after queueDirty. " +
                vbucketId.to_string());
    }
}

CheckpointManager::ItemsForCursor CheckpointManager::getNextItemsForCursor(
        CheckpointCursor* cursor, std::vector<queued_item>& items) {
    return getItemsForCursor(cursor, items, std::numeric_limits<size_t>::max());
}

CheckpointManager::ItemsForCursor CheckpointManager::getItemsForCursor(
        CheckpointCursor* cursorPtr,
        std::vector<queued_item>& items,
        size_t approxLimit) {
    LockHolder lh(queueLock);
    if (!cursorPtr) {
        EP_LOG_WARN("getItemsForCursor(): Caller had a null cursor {}",
                    vbucketId);
        return {};
    }

    auto& cursor = *cursorPtr;

    // Fetch whole checkpoints; as long as we don't exceed the approx item
    // limit.
    ItemsForCursor result((*cursor.currentCheckpoint)->getCheckpointType());

    size_t itemCount = 0;
    bool enteredNewCp = true;
    while ((result.moreAvailable = incrCursor(cursor))) {
        // We only want to return items from contiguous checkpoints with the
        // same type. We should not return Memory checkpoint items followed by
        // Disk checkpoint items or vice versa. This is due to ActiveStream
        // needing to send Disk checkpoint items as Disk snapshots to the
        // replica.
        if ((*cursor.currentCheckpoint)->getCheckpointType() !=
            result.checkpointType) {
            break;
        }
        if (enteredNewCp) {
            result.ranges.push_back(
                    {(*cursor.currentCheckpoint)->getSnapshotStartSeqno(),
                     (*cursor.currentCheckpoint)->getSnapshotEndSeqno()});
            enteredNewCp = false;
        }

        queued_item& qi = *(cursor.currentPos);
        items.push_back(qi);
        itemCount++;

        if (qi->getOperation() == queue_op::checkpoint_end) {
            enteredNewCp = true; // the next incrCuror will move to a new CP

            // Reached the end of a checkpoint; check if we have exceeded
            // our limit.
            if (itemCount >= approxLimit) {
                // Reached our limit - don't want any more items.
                // However, we *do* want to move the cursor into the next
                // checkpoint if possible; as that means the checkpoint we just
                // completed has one less cursor in it (and could potentially be
                // freed).
                moveCursorToNextCheckpoint(cursor);
                break;
            }
        }
    }

    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        std::stringstream ranges;
        for (const auto& range : result.ranges) {
            ranges << "{" << range.getStart() << "," << range.getEnd() << "}";
        }
        EP_LOG_DEBUG(
                "CheckpointManager::getItemsForCursor() "
                "cursor:{} result:{{#items:{} ranges:size:{} {} "
                "moreAvailable:{}}}",
                cursor.name,
                uint64_t(itemCount),
                result.ranges.size(),
                ranges.str(),
                result.moreAvailable ? "true" : "false");
    }

    cursor.numVisits++;

    return result;
}

bool CheckpointManager::incrCursor(CheckpointCursor &cursor) {
    if (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
        return true;
    }
    if (!moveCursorToNextCheckpoint(cursor)) {
        --(cursor.currentPos);
        return false;
    }
    return incrCursor(cursor);
}

void CheckpointManager::notifyFlusher() {
    if (flusherCB) {
        Vbid vbid = vbucketId;
        flusherCB->callback(vbid);
    }
}

void CheckpointManager::setBySeqno(int64_t seqno) {
    LockHolder lh(queueLock);
    lastBySeqno = seqno;
}

int64_t CheckpointManager::getHighSeqno() const {
    LockHolder lh(queueLock);
    return lastBySeqno;
}

int64_t CheckpointManager::nextBySeqno() {
    LockHolder lh(queueLock);
    return ++lastBySeqno;
}

void CheckpointManager::dump() const {
    std::cerr << *this << std::endl;
}

void CheckpointManager::clear(VBucket& vb, uint64_t seqno) {
    LockHolder lh(queueLock);
    clear_UNLOCKED(vb.getState(), seqno);

    // Reset the disk write queue size stat for the vbucket
    if (checkpointConfig.isPersistenceEnabled()) {
        size_t currentDqSize = vb.dirtyQueueSize.load();
        vb.dirtyQueueSize.fetch_sub(currentDqSize);
        stats.diskQueueSize.fetch_sub(currentDqSize);
    }
}

void CheckpointManager::clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno) {
    checkpointList.clear();
    numItems = 0;
    lastBySeqno.reset(seqno);
    pCursorPreCheckpointId = 0;
    addOpenCheckpoint(vbucket_state_active ? 1 : 0 /* id */,
                      lastBySeqno,
                      lastBySeqno,
                      CheckpointType::Memory);
    resetCursors();
}

void CheckpointManager::resetCursors(bool resetPersistenceCursor) {
    for (auto& cit : connCursors) {
        if (cit.second->name == pCursorName) {
            if (!resetPersistenceCursor) {
                continue;
            } else {
                uint64_t chkid = checkpointList.front()->getId();
                pCursorPreCheckpointId = chkid ? chkid - 1 : 0;
            }
        }
        cit.second->currentCheckpoint = checkpointList.begin();
        cit.second->currentPos = checkpointList.front()->begin();
        checkpointList.front()->incNumOfCursorsInCheckpoint();
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    auto& it = cursor.currentCheckpoint;
    if ((*it)->getState() == CHECKPOINT_OPEN) {
        return false;
    } else if ((*it)->getState() == CHECKPOINT_CLOSED) {
        if (std::next(it) == checkpointList.end()) {
            return false;
        }
    }

    // Remove cursor from its current checkpoint.
    (*it)->decNumOfCursorsInCheckpoint();

    // Move the cursor to the next checkpoint.
    ++it;
    cursor.currentPos = (*it)->begin();
    // Add cursor to its new current checkpoint.
    (*it)->incNumOfCursorsInCheckpoint();

    return true;
}

size_t CheckpointManager::getNumOpenChkItems() const {
    LockHolder lh(queueLock);
    return getOpenCheckpoint_UNLOCKED(lh).getNumItems();
}

uint64_t CheckpointManager::checkOpenCheckpoint_UNLOCKED(const LockHolder& lh,
                                                         bool forceCreation,
                                                         bool timeBound) {
    int checkpoint_id = 0;

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    timeBound = timeBound && (ep_real_time() - openCkpt.getCreationTime()) >=
                                     checkpointConfig.getCheckpointPeriod();
    // Create the new open checkpoint if any of the following conditions is
    // satisfied:
    // (1) force creation due to online update or high memory usage
    // (2) current checkpoint is reached to the max number of items allowed.
    // (3) time elapsed since the creation of the current checkpoint is greater
    //     than the threshold
    if (forceCreation ||
        (checkpointConfig.isItemNumBasedNewCheckpoint() &&
         openCkpt.getNumItems() >= checkpointConfig.getCheckpointMaxItems()) ||
        (openCkpt.getNumItems() > 0 && timeBound)) {
        checkpoint_id = openCkpt.getId();
        addNewCheckpoint_UNLOCKED(checkpoint_id + 1);
    }
    return checkpoint_id;
}

size_t CheckpointManager::getNumItemsForCursor(
        const CheckpointCursor* cursor) const {
    LockHolder lh(queueLock);
    return getNumItemsForCursor_UNLOCKED(cursor);
}

size_t CheckpointManager::getNumItemsForCursor_UNLOCKED(
        const CheckpointCursor* cursor) const {
    if (cursor) {
        size_t items = cursor->getRemainingItemsCount();
        CheckpointList::const_iterator chkptIterator =
                cursor->currentCheckpoint;
        if (chkptIterator != checkpointList.end()) {
            ++chkptIterator;
        }

        // Now add the item counts for all the subsequent checkpoints
        auto result = std::accumulate(
                chkptIterator,
                checkpointList.end(),
                items,
                [](size_t a, const std::unique_ptr<Checkpoint>& b) {
                    return a + b->getNumItems();
                });
        return result;
    }
    return 0;
}

void CheckpointManager::clear(vbucket_state_t vbState) {
    LockHolder lh(queueLock);
    clear_UNLOCKED(vbState, lastBySeqno);
}

bool CheckpointManager::isLastMutationItemInCheckpoint(
                                                   CheckpointCursor &cursor) {
    ChkptQueueIterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op::checkpoint_end) {
        return true;
    }
    return false;
}

void CheckpointManager::setBackfillPhase(uint64_t start, uint64_t end) {
    LockHolder lh(queueLock);
    setOpenCheckpointId_UNLOCKED(lh, 0);
    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    openCkpt.setSnapshotStartSeqno(start);
    openCkpt.setSnapshotEndSeqno(end);
}

void CheckpointManager::createSnapshot(uint64_t snapStartSeqno,
                                       uint64_t snapEndSeqno,
                                       CheckpointType checkpointType) {
    LockHolder lh(queueLock);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto openCkptId = openCkpt.getId();

    if (openCkpt.getNumItems() == 0) {
        if (openCkptId == 0) {
            setOpenCheckpointId_UNLOCKED(lh, openCkptId + 1);
            resetCursors(false);
        }
        openCkpt.setSnapshotStartSeqno(snapStartSeqno);
        openCkpt.setSnapshotEndSeqno(snapEndSeqno);
        openCkpt.setCheckpointType(checkpointType);
        return;
    }

    addNewCheckpoint_UNLOCKED(
            openCkptId + 1, snapStartSeqno, snapEndSeqno, checkpointType);
}

void CheckpointManager::resetSnapshotRange() {
    LockHolder lh(queueLock);

    checkpointList.back()->setSnapshotStartSeqno(
            static_cast<uint64_t>(lastBySeqno));
    checkpointList.back()->setSnapshotEndSeqno(
            static_cast<uint64_t>(lastBySeqno));
}

void CheckpointManager::updateCurrentSnapshot(uint64_t snapEnd,
                                              CheckpointType checkpointType) {
    LockHolder lh(queueLock);

    auto& ckpt = getOpenCheckpoint_UNLOCKED(lh);
    ckpt.setSnapshotEndSeqno(snapEnd);
    ckpt.setCheckpointType(checkpointType);
}

snapshot_info_t CheckpointManager::getSnapshotInfo() {
    LockHolder lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    snapshot_info_t info(
            lastBySeqno,
            {openCkpt.getSnapshotStartSeqno(), openCkpt.getSnapshotEndSeqno()});

    // If there are no items in the open checkpoint then we need to resume by
    // using that sequence numbers of the last closed snapshot. The exception is
    // if we are in a partial snapshot which can be detected by checking if the
    // snapshot start sequence number is greater than the start sequence number
    // Also, since the last closed snapshot may not be in the checkpoint manager
    // we should just use the last by sequence number. The open checkpoint will
    // be overwritten once the next snapshot marker is received since there are
    // no items in it.
    if (openCkpt.getNumItems() == 0 &&
        static_cast<uint64_t>(lastBySeqno) < info.range.getStart()) {
        info.range = snapshot_range_t(lastBySeqno, lastBySeqno);
    }

    return info;
}

uint64_t CheckpointManager::getOpenSnapshotStartSeqno() const {
    LockHolder lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    return openCkpt.getSnapshotStartSeqno();
}

void CheckpointManager::checkAndAddNewCheckpoint() {
    LockHolder lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto openCkptId = openCkpt.getId();

    // This function is executed only on a DCP Consumer at snapshot-end
    // mutation. So, by logic a non-backfill open checkpoint cannot be empty.
    Expects(openCkpt.getNumItems() > 0 || openCkptId == 0);

    // If the open checkpoint is the backfill-snapshot (checkpoint-id=0),
    // then we just update the id of the existing checkpoint and we update
    // cursors.
    // Notes:
    //     - we need this because the (checkpoint-id = 0) is reserved for the
    //         backfill phase, and any attempt of stream-request to a
    //         replica-vbucket (e.g., View-Engine) fails if
    //         (current-checkpoint-id = 0). There are also some PassiveStream
    //         tests relying on that.
    //     - an alternative to this is closing the checkpoint and adding a
    //         new one.
    //     - the backfill checkpoint is empty by definition
    if (openCkptId == 0) {
        setOpenCheckpointId_UNLOCKED(lh, openCkptId + 1);
        resetCursors(false);
        return;
    }

    addNewCheckpoint_UNLOCKED(openCkptId + 1);
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id,
                                                    Vbid vbid,
                                                    queue_op checkpoint_op) {
    uint64_t bySeqno;
    StoredDocKey key(to_string(checkpoint_op), CollectionID::System);

    switch (checkpoint_op) {
    case queue_op::checkpoint_start:
        bySeqno = lastBySeqno + 1;
        break;
    case queue_op::checkpoint_end:
        bySeqno = lastBySeqno;
        break;
    case queue_op::empty:
        bySeqno = lastBySeqno;
        break;
    case queue_op::set_vbucket_state:
        bySeqno = lastBySeqno + 1;
        break;

    default:
        throw std::invalid_argument("CheckpointManager::createCheckpointItem:"
                        "checkpoint_op (which is " +
                        std::to_string(static_cast<std::underlying_type<queue_op>::type>(checkpoint_op)) +
                        ") is not a valid item to create");
    }

    queued_item qi(new Item(key, vbid, checkpoint_op, id, bySeqno));
    return qi;
}

uint64_t CheckpointManager::createNewCheckpoint() {
    LockHolder lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    if (openCkpt.getNumItems() == 0) {
        return openCkpt.getId();
    }

    addNewCheckpoint_UNLOCKED(openCkpt.getId() + 1);
    return getOpenCheckpointId_UNLOCKED(lh);
}

uint64_t CheckpointManager::getPersistenceCursorPreChkId() {
    LockHolder lh(queueLock);
    return pCursorPreCheckpointId;
}

void CheckpointManager::itemsPersisted() {
    LockHolder lh(queueLock);
    auto itr = persistenceCursor->currentCheckpoint;
    pCursorPreCheckpointId = ((*itr)->getId() > 0) ? (*itr)->getId() - 1 : 0;
}

size_t CheckpointManager::getMemoryUsage_UNLOCKED() const {
    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        memUsage += checkpoint->getMemConsumption();
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryOverhead_UNLOCKED() const {
    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        memUsage += checkpoint->getMemoryOverhead();
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryUsage() const {
    LockHolder lh(queueLock);
    return getMemoryUsage_UNLOCKED();
}

size_t CheckpointManager::getMemoryUsageOfUnrefCheckpoints() const {
    LockHolder lh(queueLock);

    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        if (checkpoint->isNoCursorsInCheckpoint()) {
            memUsage += checkpoint->getMemConsumption();
        } else {
            break;
        }
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryOverhead() const {
    LockHolder lh(queueLock);
    return getMemoryOverhead_UNLOCKED();
}

void CheckpointManager::addStats(const AddStatFn& add_stat,
                                 const void* cookie) {
    LockHolder lh(queueLock);
    char buf[256];

    try {
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:open_checkpoint_id", vbucketId.get());
        add_casted_stat(
                buf, getOpenCheckpointId_UNLOCKED(lh), add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:last_closed_checkpoint_id",
                         vbucketId.get());
        add_casted_stat(
                buf, getLastClosedCheckpointId_UNLOCKED(lh), add_stat, cookie);
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:num_conn_cursors", vbucketId.get());
        add_casted_stat(buf, connCursors.size(), add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:num_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf, numItems, add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:num_open_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf,
                        getOpenCheckpoint_UNLOCKED(lh).getNumItems(),
                        add_stat,
                        cookie);
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:num_checkpoints", vbucketId.get());
        add_casted_stat(buf, checkpointList.size(), add_stat, cookie);

        if (persistenceCursor) {
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:num_items_for_persistence",
                             vbucketId.get());
            add_casted_stat(buf,
                            getNumItemsForCursor_UNLOCKED(persistenceCursor),
                            add_stat,
                            cookie);
        }
        checked_snprintf(buf, sizeof(buf), "vb_%d:mem_usage", vbucketId.get());
        add_casted_stat(buf, getMemoryUsage_UNLOCKED(), add_stat, cookie);

        for (const auto& cursor : connCursors) {
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:cursor_checkpoint_id",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf,
                            (*(cursor.second->currentCheckpoint))->getId(),
                            add_stat,
                            cookie);
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:cursor_seqno",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf,
                            (*(cursor.second->currentPos))->getBySeqno(),
                            add_stat,
                            cookie);
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:num_visits",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(
                    buf, cursor.second->numVisits.load(), add_stat, cookie);
            if (cursor.second.get() != persistenceCursor) {
                checked_snprintf(buf,
                                 sizeof(buf),
                                 "vb_%d:%s:num_items_for_cursor",
                                 vbucketId.get(),
                                 cursor.second->name.c_str());
                add_casted_stat(
                        buf,
                        getNumItemsForCursor_UNLOCKED(cursor.second.get()),
                        add_stat,
                        cookie);
            }
        }
    } catch (std::exception& error) {
        EP_LOG_WARN(
                "CheckpointManager::addStats: An error occurred while adding "
                "stats: {}",
                error.what());
    }
}

void CheckpointManager::takeAndResetCursors(CheckpointManager& other) {
    pCursor = other.pCursor;
    persistenceCursor = pCursor.lock().get();
    for (auto& cursor : other.connCursors) {
        connCursors[cursor.second->name] = cursor.second;
    }
    other.connCursors.clear();

    resetCursors(true /*reset persistence*/);
}

bool CheckpointManager::isOpenCheckpointDisk() {
    return checkpointList.back()->isDiskCheckpoint();
}

std::ostream& operator <<(std::ostream& os, const CheckpointManager& m) {
    os << "CheckpointManager[" << &m << "] with numItems:"
       << m.getNumItems() << " checkpoints:" << m.checkpointList.size()
       << std::endl;
    for (const auto& c : m.checkpointList) {
        os << "    " << *c << std::endl;
    }
    os << "    connCursors:[" << std::endl;
    for (const auto cur : m.connCursors) {
        os << "        " << cur.first << ": " << *cur.second << std::endl;
    }
    os << "    ]" << std::endl;
    return os;
}
