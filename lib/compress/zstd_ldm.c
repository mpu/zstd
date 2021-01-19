/*
 * Copyright (c) 2016-2021, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

#include "zstd_ldm.h"

#include "../common/debug.h"
#include "xxhash.h"
#include "zstd_fast.h"          /* ZSTD_fillHashTable() */
#include "zstd_double_fast.h"   /* ZSTD_fillDoubleHashTable() */
#include "zstd_ldm_geartab.h"

#define LDM_BUCKET_SIZE_LOG 3
#define LDM_MIN_MATCH_LENGTH 64
#define LDM_HASH_RLOG 7
#define LDM_LOOKAHEAD_SPLITS 64

#if 1
typedef struct {
    U64 rolling;
    U64 stopMask;
} ldmRollingHashState_t;

typedef struct {
    U64 rollingHash;
    size_t position;
} ldmSplit_t;

static void ZSTD_ldm_gear_init(ldmRollingHashState_t* state, ldmParams_t const* params)
{
    state->rolling = ~(U32)0;

    {
        unsigned maxBitsInMask = MIN(params->minMatchLength, 64);
        unsigned minBitsInMask = params->hashRateLog;
        U64 mask;

        if (minBitsInMask > 0 && minBitsInMask <= maxBitsInMask) {
            mask = (((U64)1 << minBitsInMask) - 1) << (maxBitsInMask - minBitsInMask);
        } else {
            /* In this degenerate case we choose to honor the hash rate. */
            mask = ((U64)1 << minBitsInMask) - 1;
        }
        state->stopMask = mask;
    }
}

static size_t ZSTD_ldm_gear_feed(ldmRollingHashState_t* state,
                                 BYTE const* data, size_t size,
                                 size_t* splits, unsigned* numSplits)
{
    size_t n;
    U64 hash, mask;

    hash = state->rolling;
    mask = state->stopMask;
    n = 0;

#define GEAR_ITER_ONCE() do { \
        hash = (hash << 1) + ZSTD_ldm_gearTab[data[n] & 0xff]; \
        n += 1; \
        if (UNLIKELY((hash & mask) == 0)) { \
            splits[*numSplits] = n; \
            *numSplits += 1; \
            if (*numSplits == LDM_LOOKAHEAD_SPLITS) \
                goto done; \
        } \
    } while (0)

    while (n + 3 < size) {
        GEAR_ITER_ONCE();
        GEAR_ITER_ONCE();
        GEAR_ITER_ONCE();
        GEAR_ITER_ONCE();
    }
    while (n < size) {
        GEAR_ITER_ONCE();
    }

#undef GEAR_ITER_ONCE

done:
    state->rolling = hash;
    return n;
}

#else

typedef struct {
    U64 lane[4];       /* rolling hash lanes */
    unsigned curLane;  /* current lane; can be 0, 1, 2, or 3 */
    U64 stopMask;      /* stop as soon as the current thread matches this mask */
} ldmRollingHashState_t;

static void ZSTD_ldm_gear_init(ldmRollingHashState_t* state, ldmParams_t const* params)
{
    state->lane[0] = ~(U32)0;
    state->lane[1] = ~(U32)0;
    state->lane[2] = ~(U32)0;
    state->lane[3] = ~(U32)0;

    state->curLane = 0;

    /* We want to use a mask that depends on no more and, if possible, no less
     * than minMatchLength bytes of the input. With vanilla gear hash, bit n
     * (0 being the lsb) depends on the (n+1) previous bytes. With the threaded
     * gear hash implementation we use, bit n depends on no more than the 4(n+1)
     * previous bytes. */
    {
        unsigned maxBitsInMask = MIN(params->minMatchLength / 4, 64);
        unsigned minBitsInMask = params->hashRateLog;
        U64 mask;

        if (minBitsInMask > 0 && minBitsInMask <= maxBitsInMask) {
            mask = (((U64)1 << minBitsInMask) - 1) << (maxBitsInMask - minBitsInMask);
        } else {
            /* In this degenerate case we choose to honor the hash rate. */
            mask = ((U64)1 << minBitsInMask) - 1;
        }
        state->stopMask = mask;
    }
}

static size_t ZSTD_ldm_gear_feed(ldmRollingHashState_t* state,
                                 BYTE const* data, size_t size,
                                 size_t* splits, unsigned* numSplits)
{
    size_t n;
    U64 mask;
    unsigned l;

    l = state->curLane;
    mask = state->stopMask;
    n = 0;

    if (size > 4) {
        U64 u0, u1, u2, u3;
        U64 v0, v1, v2, v3;

        u0 = state->lane[(l + 0) & 3];
        u1 = state->lane[(l + 1) & 3];
        u2 = state->lane[(l + 2) & 3];
        u3 = state->lane[(l + 3) & 3];

        do {
            u0 = (u0 << 1) + ZSTD_ldm_gearTab[data[n+0] & 0xff];
            u1 = (u1 << 1) + ZSTD_ldm_gearTab[data[n+1] & 0xff];
            u2 = (u2 << 1) + ZSTD_ldm_gearTab[data[n+2] & 0xff];
            u3 = (u3 << 1) + ZSTD_ldm_gearTab[data[n+3] & 0xff];

            if (UNLIKELY((u0 & mask) == 0)) {
                splits[*numSplits] = n + 1;
                *numSplits += 1;
                if (*numSplits == LDM_LOOKAHEAD_SPLITS) {
                    n += 1;
                    break;
                }
            }

            if (UNLIKELY((u1 & mask) == 0)) {
                splits[*numSplits] = n + 2;
                *numSplits += 1;
                if (*numSplits == LDM_LOOKAHEAD_SPLITS) {
                    n += 2;
                    break;
                }
            }

            if (UNLIKELY((u2 & mask) == 0)) {
                splits[*numSplits] = n + 3;
                *numSplits += 1;
                if (*numSplits == LDM_LOOKAHEAD_SPLITS) {
                    n += 3;
                    break;
                }
            }

            if (UNLIKELY((u3 & mask) == 0)) {
                splits[*numSplits] = n + 4;
                *numSplits += 1;
                if (*numSplits == LDM_LOOKAHEAD_SPLITS) {
                    n += 4;
                    break;
                }
            }

            n += 4;
        } while (n + 4 <= size);

        state->lane[(l + 0) & 3] = u0;
        state->lane[(l + 1) & 3] = u1;
        state->lane[(l + 2) & 3] = u2;
        state->lane[(l + 3) & 3] = u3;

        if (*numSplits == LDM_LOOKAHEAD_SPLITS) {
            state->curLane = (l + n) & 3;
            return n;
        }

        assert(l == ((l + n) & 3));
    }

    while (n < size) {
        state->lane[l] = (state->lane[l] << 1) + ZSTD_ldm_gearTab[data[n] & 0xff];
        n++;
        if ((state->lane[l] & mask) == 0) {
            splits[*numSplits] = n;
            *numSplits += 1;
            if (*numSplits == LDM_LOOKAHEAD_SPLITS)
                break;
        }
        l = (l + 1) & 3;
    }
    state->curLane = l;
    return n;
}
#endif

void ZSTD_ldm_adjustParameters(ldmParams_t* params,
                               ZSTD_compressionParameters const* cParams)
{
    params->windowLog = cParams->windowLog;
    ZSTD_STATIC_ASSERT(LDM_BUCKET_SIZE_LOG <= ZSTD_LDM_BUCKETSIZELOG_MAX);
    DEBUGLOG(4, "ZSTD_ldm_adjustParameters");
    if (!params->bucketSizeLog) params->bucketSizeLog = LDM_BUCKET_SIZE_LOG;
    if (!params->minMatchLength) params->minMatchLength = LDM_MIN_MATCH_LENGTH;
    if (params->hashLog == 0) {
        params->hashLog = MAX(ZSTD_HASHLOG_MIN, params->windowLog - LDM_HASH_RLOG);
        assert(params->hashLog <= ZSTD_HASHLOG_MAX);
    }
    if (params->hashRateLog == 0) {
        params->hashRateLog = params->windowLog < params->hashLog
                                   ? 0
                                   : params->windowLog - params->hashLog;
    }
    params->bucketSizeLog = MIN(params->bucketSizeLog, params->hashLog);
}

size_t ZSTD_ldm_getTableSize(ldmParams_t params)
{
    size_t const ldmHSize = ((size_t)1) << params.hashLog;
    size_t const ldmBucketSizeLog = MIN(params.bucketSizeLog, params.hashLog);
    size_t const ldmBucketSize = ((size_t)1) << (params.hashLog - ldmBucketSizeLog);
    size_t const totalSize = ZSTD_cwksp_alloc_size(ldmBucketSize)
                           + ZSTD_cwksp_alloc_size(ldmHSize * sizeof(ldmEntry_t));
    return params.enableLdm ? totalSize : 0;
}

size_t ZSTD_ldm_getMaxNbSeq(ldmParams_t params, size_t maxChunkSize)
{
    return params.enableLdm ? (maxChunkSize / params.minMatchLength) : 0;
}

/** ZSTD_ldm_getSmallHash() :
 *  numBits should be <= 32
 *  If numBits==0, returns 0.
 *  @return : the most significant numBits of hash. */
static U32 ZSTD_ldm_getSmallHash(U64 hash, U32 numBits)
{
    assert(numBits <= 32);
    return numBits == 0 ? 0 : (U32)(hash >> (64 - numBits));
}

/** ZSTD_ldm_getChecksum() :
 *  numBitsToDiscard should be <= 32
 *  @return : the next most significant 32 bits after numBitsToDiscard */
static U32 ZSTD_ldm_getChecksum(U64 hash, U32 numBitsToDiscard)
{
    assert(numBitsToDiscard <= 32);
    return (hash >> (64 - (32 + numBitsToDiscard))) & 0xFFFFFFFF;
}

/** ZSTD_ldm_getTagMask() :
 *  Returns the mask against which the rolling hash must be
 *  checked. */
static U64 ZSTD_ldm_getTagMask(U32 hbits, U32 hashRateLog)
{
    assert(hashRateLog < 32 && hbits <= 32);
    if (32 - hbits < hashRateLog) {
        return (((U64)1 << hashRateLog) - 1);
    } else {
        return (((U64)1 << hashRateLog) - 1) << (32 - hbits - hashRateLog);
    }
}

/** ZSTD_ldm_getBucket() :
 *  Returns a pointer to the start of the bucket associated with hash. */
static ldmEntry_t* ZSTD_ldm_getBucket(
        ldmState_t* ldmState, size_t hash, ldmParams_t const ldmParams)
{
    return ldmState->hashTable + (hash << ldmParams.bucketSizeLog);
}

/** ZSTD_ldm_insertEntry() :
 *  Insert the entry with corresponding hash into the hash table */
static void ZSTD_ldm_insertEntry(ldmState_t* ldmState,
                                 size_t const hash, const ldmEntry_t entry,
                                 ldmParams_t const ldmParams)
{
    BYTE* const pOffset = ldmState->bucketOffsets + hash;
    unsigned const offset = *pOffset;

    *(ZSTD_ldm_getBucket(ldmState, hash, ldmParams) + offset) = entry;
    *pOffset = (BYTE)((offset + 1) & ((1u << ldmParams.bucketSizeLog) - 1));

}

/** ZSTD_ldm_makeEntryAndInsertByTag() :
 *
 *  Gets the small hash, checksum, and tag from the rollingHash.
 *
 *  If the tag matches (1 << ldmParams.hashRateLog)-1, then
 *  creates an ldmEntry from the offset, and inserts it into the hash table.
 *
 *  hBits is the length of the small hash, which is the most significant hBits
 *  of rollingHash. The checksum is the next 32 most significant bits, followed
 *  by ldmParams.hashRateLog bits that make up the tag. */
static void ZSTD_ldm_makeEntryAndInsertByTag(ldmState_t* ldmState,
                                             U64 const rollingHash,
                                             U32 const hBits,
                                             U32 const offset,
                                             ldmParams_t const ldmParams)
{
    U64 const tagMask = ZSTD_ldm_getTagMask(hBits, ldmParams.hashRateLog);
    if ((rollingHash & tagMask) == tagMask) {
        U32 const hash = ZSTD_ldm_getSmallHash(rollingHash, hBits);
        U32 const checksum = ZSTD_ldm_getChecksum(rollingHash, hBits);
        ldmEntry_t entry;
        entry.offset = offset;
        entry.checksum = checksum;
        ZSTD_ldm_insertEntry(ldmState, hash, entry, ldmParams);
    }
}

/** ZSTD_ldm_countBackwardsMatch() :
 *  Returns the number of bytes that match backwards before pIn and pMatch.
 *
 *  We count only bytes where pMatch >= pBase and pIn >= pAnchor. */
static size_t ZSTD_ldm_countBackwardsMatch(
            const BYTE* pIn, const BYTE* pAnchor,
            const BYTE* pMatch, const BYTE* pMatchBase)
{
    size_t matchLength = 0;
    while (pIn > pAnchor && pMatch > pMatchBase && pIn[-1] == pMatch[-1]) {
        pIn--;
        pMatch--;
        matchLength++;
    }
    return matchLength;
}

/** ZSTD_ldm_countBackwardsMatch_2segments() :
 *  Returns the number of bytes that match backwards from pMatch,
 *  even with the backwards match spanning 2 different segments.
 *
 *  On reaching `pMatchBase`, start counting from mEnd */
static size_t ZSTD_ldm_countBackwardsMatch_2segments(
                    const BYTE* pIn, const BYTE* pAnchor,
                    const BYTE* pMatch, const BYTE* pMatchBase,
                    const BYTE* pExtDictStart, const BYTE* pExtDictEnd)
{
    size_t matchLength = ZSTD_ldm_countBackwardsMatch(pIn, pAnchor, pMatch, pMatchBase);
    if (pMatch - matchLength != pMatchBase || pMatchBase == pExtDictStart) {
        /* If backwards match is entirely in the extDict or prefix, immediately return */
        return matchLength;
    }
    DEBUGLOG(7, "ZSTD_ldm_countBackwardsMatch_2segments: found 2-parts backwards match (length in prefix==%zu)", matchLength);
    matchLength += ZSTD_ldm_countBackwardsMatch(pIn - matchLength, pAnchor, pExtDictEnd, pExtDictStart);
    DEBUGLOG(7, "final backwards match length = %zu", matchLength);
    return matchLength;
}

/** ZSTD_ldm_fillFastTables() :
 *
 *  Fills the relevant tables for the ZSTD_fast and ZSTD_dfast strategies.
 *  This is similar to ZSTD_loadDictionaryContent.
 *
 *  The tables for the other strategies are filled within their
 *  block compressors. */
static size_t ZSTD_ldm_fillFastTables(ZSTD_matchState_t* ms,
                                      void const* end)
{
    const BYTE* const iend = (const BYTE*)end;

    switch(ms->cParams.strategy)
    {
    case ZSTD_fast:
        ZSTD_fillHashTable(ms, iend, ZSTD_dtlm_fast);
        break;

    case ZSTD_dfast:
        ZSTD_fillDoubleHashTable(ms, iend, ZSTD_dtlm_fast);
        break;

    case ZSTD_greedy:
    case ZSTD_lazy:
    case ZSTD_lazy2:
    case ZSTD_btlazy2:
    case ZSTD_btopt:
    case ZSTD_btultra:
    case ZSTD_btultra2:
        break;
    default:
        assert(0);  /* not possible : not a valid strategy id */
    }

    return 0;
}

/** ZSTD_ldm_fillLdmHashTable() :
 *
 *  Fills hashTable from (lastHashed + 1) to iend (non-inclusive).
 *  lastHash is the rolling hash that corresponds to lastHashed.
 *
 *  Returns the rolling hash corresponding to position iend-1. */
static U64 ZSTD_ldm_fillLdmHashTable(ldmState_t* state,
                                     U64 lastHash, const BYTE* lastHashed,
                                     const BYTE* iend, const BYTE* base,
                                     U32 hBits, ldmParams_t const ldmParams)
{
    U64 rollingHash = lastHash;
    const BYTE* cur = lastHashed + 1;

    while (cur < iend) {
        rollingHash = ZSTD_rollingHash_rotate(rollingHash, cur[-1],
                                              cur[ldmParams.minMatchLength-1],
                                              state->hashPower);
        ZSTD_ldm_makeEntryAndInsertByTag(state,
                                         rollingHash, hBits,
                                         (U32)(cur - base), ldmParams);
        ++cur;
    }
    return rollingHash;
}

/* TODO: See where this is used */
void ZSTD_ldm_fillHashTable(
            ldmState_t* state, const BYTE* ip,
            const BYTE* iend, ldmParams_t const* params)
{
    DEBUGLOG(5, "ZSTD_ldm_fillHashTable");
    if ((size_t)(iend - ip) >= params->minMatchLength) {
        U64 startingHash = ZSTD_rollingHash_compute(ip, params->minMatchLength);
        ZSTD_ldm_fillLdmHashTable(
            state, startingHash, ip, iend - params->minMatchLength, state->window.base,
            params->hashLog - params->bucketSizeLog,
            *params);
    }
}


/** ZSTD_ldm_limitTableUpdate() :
 *
 *  Sets cctx->nextToUpdate to a position corresponding closer to anchor
 *  if it is far way
 *  (after a long match, only update tables a limited amount). */
static void ZSTD_ldm_limitTableUpdate(ZSTD_matchState_t* ms, const BYTE* anchor)
{
    U32 const curr = (U32)(anchor - ms->window.base);
    if (curr > ms->nextToUpdate + 1024) {
        ms->nextToUpdate =
            curr - MIN(512, curr - ms->nextToUpdate - 1024);
    }
}

static size_t ZSTD_ldm_generateSequences_internal(
        ldmState_t* ldmState, rawSeqStore_t* rawSeqStore,
        ldmParams_t const* params, void const* src, size_t srcSize)
{
    /* LDM parameters */
    int const extDict = ZSTD_window_hasExtDict(ldmState->window);
    U32 const minMatchLength = params->minMatchLength;
    U32 const entsPerBucket = 1U << params->bucketSizeLog;
    U32 const hBits = params->hashLog - params->bucketSizeLog;
    /* Prefix and extDict parameters */
    U32 const dictLimit = ldmState->window.dictLimit;
    U32 const lowestIndex = extDict ? ldmState->window.lowLimit : dictLimit;
    BYTE const* const base = ldmState->window.base;
    BYTE const* const dictBase = extDict ? ldmState->window.dictBase : NULL;
    BYTE const* const dictStart = extDict ? dictBase + lowestIndex : NULL;
    BYTE const* const dictEnd = extDict ? dictBase + dictLimit : NULL;
    BYTE const* const lowPrefixPtr = base + dictLimit;
    /* Input bounds */
    BYTE const* const istart = (BYTE const*)src;
    BYTE const* const iend = istart + srcSize;
    BYTE const* const ilimit = iend - HASH_READ_SIZE;
    /* Input positions */
    BYTE const* anchor = istart;
    BYTE const* ip = istart;
    /* Rolling hash state */
    ldmRollingHashState_t hashState;
    /* Pipeline arrays */
    size_t splits[LDM_LOOKAHEAD_SPLITS];
    struct {
        BYTE const* split;
        U32 hash;
        U32 checksum;
        ldmEntry_t* bucket;
    } candidates[LDM_LOOKAHEAD_SPLITS];
    unsigned numSplits;

    /* Initialize the rolling hash state with the first minMatchLength
     * bytes */
    ZSTD_ldm_gear_init(&hashState, params);
    {
        size_t n = 0;

        while (n < minMatchLength) {
            numSplits = 0;
            n += ZSTD_ldm_gear_feed(&hashState, ip + n, minMatchLength - n,
                                    splits, &numSplits);
        }
        ip += minMatchLength;
    }

    while (ip < ilimit) {
        size_t hashed;
        unsigned n;

        numSplits = 0;
        hashed = ZSTD_ldm_gear_feed(&hashState, ip, ilimit - ip,
                                    splits, &numSplits);

        for (n = 0; n < numSplits; n++) {
            BYTE const* const split = ip + splits[n] - minMatchLength;
            U64 const xxhash = XXH64(split, minMatchLength, 0);
            U64 const hash = xxhash & (((U32)1 << hBits) - 1);

            candidates[n].split = split;
            candidates[n].hash = hash;
            candidates[n].checksum = (U32)(xxhash >> 32);
            candidates[n].bucket = ZSTD_ldm_getBucket(ldmState, hash, *params);
        }

        for (n = 0; n < numSplits; n++) {
            size_t forwardMatchLength = 0, backwardMatchLength = 0,
                   bestMatchLength = 0;
            BYTE const* const split = candidates[n].split;
            U64 const checksum = candidates[n].checksum;
            ldmEntry_t* const bucket = candidates[n].bucket;
            ldmEntry_t const* cur;
            ldmEntry_t const* bestEntry = NULL;

            for (cur = bucket; cur < bucket + entsPerBucket; cur++) {
                size_t curForwardMatchLength, curBackwardMatchLength,
                       curTotalMatchLength;
                if (cur->checksum != checksum || cur->offset <= lowestIndex) {
                    continue;
                }
                if (extDict) {
                    BYTE const* const curMatchBase =
                        cur->offset < dictLimit ? dictBase : base;
                    BYTE const* const pMatch = curMatchBase + cur->offset;
                    BYTE const* const matchEnd =
                        cur->offset < dictLimit ? dictEnd : iend;
                    BYTE const* const lowMatchPtr =
                        cur->offset < dictLimit ? dictStart : lowPrefixPtr;
                    curForwardMatchLength =
                        ZSTD_count_2segments(split, pMatch, iend, matchEnd, lowPrefixPtr);
                    if (curForwardMatchLength < minMatchLength) {
                        continue;
                    }
                    curBackwardMatchLength = ZSTD_ldm_countBackwardsMatch_2segments(
                            split, anchor, pMatch, lowMatchPtr, dictStart, dictEnd);
                } else { /* !extDict */
                    BYTE const* const pMatch = base + cur->offset;
                    curForwardMatchLength = ZSTD_count(split, pMatch, iend);
                    if (curForwardMatchLength < minMatchLength) {
                        continue;
                    }
                    curBackwardMatchLength =
                        ZSTD_ldm_countBackwardsMatch(split, anchor, pMatch, lowPrefixPtr);
                }
                curTotalMatchLength = curForwardMatchLength + curBackwardMatchLength;

                if (curTotalMatchLength > bestMatchLength) {
                    bestMatchLength = curTotalMatchLength;
                    forwardMatchLength = curForwardMatchLength;
                    backwardMatchLength = curBackwardMatchLength;
                    bestEntry = cur;
                }
            }

            /* No match found -- insert an entry into the hash table
             * and process the next split */
            if (bestEntry == NULL) {
                ldmEntry_t entry;

                entry.offset = (U32)(split - base);
                entry.checksum = checksum;
                ZSTD_ldm_insertEntry(ldmState, candidates[n].hash, entry, *params);
                continue;
            }

            /* Match! */
        }

        ip += hashed;
    }

    return iend - anchor;
}

#if 0
static size_t ZSTD_ldm_generateSequences_internal(
        ldmState_t* ldmState, rawSeqStore_t* rawSeqStore,
        ldmParams_t const* params, void const* src, size_t srcSize)
{
    /* LDM parameters */
    int const extDict = ZSTD_window_hasExtDict(ldmState->window);
    U32 const minMatchLength = params->minMatchLength;
    // U64 const hashPower = ldmState->hashPower;
    U32 const hBits = params->hashLog - params->bucketSizeLog;
    U32 const entsPerBucket = 1U << params->bucketSizeLog;
    U64 const tagMask = ZSTD_ldm_getTagMask(hBits, params->hashRateLog);
    /* Prefix and extDict parameters */
    U32 const dictLimit = ldmState->window.dictLimit;
    U32 const lowestIndex = extDict ? ldmState->window.lowLimit : dictLimit;
    BYTE const* const base = ldmState->window.base;
    BYTE const* const dictBase = extDict ? ldmState->window.dictBase : NULL;
    BYTE const* const dictStart = extDict ? dictBase + lowestIndex : NULL;
    BYTE const* const dictEnd = extDict ? dictBase + dictLimit : NULL;
    BYTE const* const lowPrefixPtr = base + dictLimit;
    /* Input bounds */
    BYTE const* const istart = (BYTE const*)src;
    BYTE const* const iend = istart + srcSize;
    BYTE const* const ilimit = iend - MAX(minMatchLength, HASH_READ_SIZE);
    /* Input positions */
    BYTE const* anchor = istart;
    BYTE const* ip = istart;
    /* Rolling hash */
    BYTE const* lastHashed = NULL;
    U64 rollingHash = 0;

    while (ip <= ilimit) {
        U32 const currentIndex = (U32)(ip - base);
        U32 hash, checksum;
        size_t mLength;
        size_t forwardMatchLength = 0, backwardMatchLength = 0;
        ldmEntry_t const* bestEntry = NULL;
        ldmEntry_t newEntry;

        if (ip != istart) {
#if 0
            rollingHash = ZSTD_rollingHash_rotate(rollingHash, lastHashed[0],
                                                  lastHashed[minMatchLength],
                                                  hashPower);
#endif
           rollingHash <<= 1;
           rollingHash += ZSTD_ldm_gearTab[lastHashed[minMatchLength]];
        } else {
#if 0
            rollingHash = ZSTD_rollingHash_compute(ip, minMatchLength);
#endif
            unsigned i;

            for (i = 0; i < minMatchLength; i++) {
               rollingHash <<= 1;
               rollingHash += ZSTD_ldm_gearTab[ip[i]];
            }
        }
        lastHashed = ip;

        /* Do not insert and do not look for a match */
        if ((rollingHash & tagMask) != tagMask) {
           ip++;
           continue;
        }

        hash = ZSTD_ldm_getSmallHash(rollingHash, hBits);
        checksum = ZSTD_ldm_getChecksum(rollingHash, hBits);

        newEntry.offset = currentIndex;
        newEntry.checksum = checksum;

        /* Get the best entry and compute the match lengths */
        {
            ldmEntry_t* const bucket = ZSTD_ldm_getBucket(ldmState, hash, *params);
            ldmEntry_t const* cur;
            size_t bestMatchLength = 0;

            for (cur = bucket; cur < bucket + entsPerBucket; ++cur) {
                size_t curForwardMatchLength, curBackwardMatchLength,
                       curTotalMatchLength;
                if (cur->checksum != checksum || cur->offset <= lowestIndex) {
                    continue;
                }
                if (extDict) {
                    BYTE const* const curMatchBase =
                        cur->offset < dictLimit ? dictBase : base;
                    BYTE const* const pMatch = curMatchBase + cur->offset;
                    BYTE const* const matchEnd =
                        cur->offset < dictLimit ? dictEnd : iend;
                    BYTE const* const lowMatchPtr =
                        cur->offset < dictLimit ? dictStart : lowPrefixPtr;

                    curForwardMatchLength = ZSTD_count_2segments(
                                                ip, pMatch, iend,
                                                matchEnd, lowPrefixPtr);
                    if (curForwardMatchLength < minMatchLength) {
                        continue;
                    }
                    curBackwardMatchLength =
                        ZSTD_ldm_countBackwardsMatch_2segments(ip, anchor,
                                                               pMatch, lowMatchPtr,
                                                               dictStart, dictEnd);
                    curTotalMatchLength = curForwardMatchLength +
                                          curBackwardMatchLength;
                } else { /* !extDict */
                    BYTE const* const pMatch = base + cur->offset;
                    curForwardMatchLength = ZSTD_count(ip, pMatch, iend);
                    if (curForwardMatchLength < minMatchLength) {
                        continue;
                    }
                    curBackwardMatchLength =
                        ZSTD_ldm_countBackwardsMatch(ip, anchor, pMatch,
                                                     lowPrefixPtr);
                    curTotalMatchLength = curForwardMatchLength +
                                          curBackwardMatchLength;
                }

                if (curTotalMatchLength > bestMatchLength) {
                    bestMatchLength = curTotalMatchLength;
                    forwardMatchLength = curForwardMatchLength;
                    backwardMatchLength = curBackwardMatchLength;
                    bestEntry = cur;
                }
            }
        }

        /* No match found -- continue searching */
        if (bestEntry == NULL) {
            ZSTD_ldm_insertEntry(ldmState, hash, newEntry, *params);
            ip++;
            continue;
        }

        /* Match found */
        mLength = forwardMatchLength + backwardMatchLength;
        ip -= backwardMatchLength;

        {
            /* Store the sequence:
             * ip = currentIndex - backwardMatchLength
             * The match is at (bestEntry->offset - backwardMatchLength)
             */
            U32 const matchIndex = bestEntry->offset;
            U32 const offset = currentIndex - matchIndex;
            rawSeq* const seq = rawSeqStore->seq + rawSeqStore->size;

            /* Out of sequence storage */
            if (rawSeqStore->size == rawSeqStore->capacity)
                return ERROR(dstSize_tooSmall);
            seq->litLength = (U32)(ip - anchor);
            seq->matchLength = (U32)mLength;
            seq->offset = offset;
            rawSeqStore->size++;
        }

        /* Insert the current entry into the hash table --- it must be
         * done after the previous block to avoid clobbering bestEntry */
        ZSTD_ldm_insertEntry(ldmState, hash, newEntry, *params);

        assert(ip + backwardMatchLength == lastHashed);

        /* Fill the hash table from lastHashed+1 to ip+mLength*/
        /* Heuristic: don't need to fill the entire table at end of block */
        if (ip + mLength <= ilimit) {
            rollingHash = ZSTD_ldm_fillLdmHashTable(
                              ldmState, rollingHash, lastHashed,
                              ip + mLength, base, hBits, *params);
            lastHashed = ip + mLength - 1;
        }
        ip += mLength;
        anchor = ip;
    }
    return iend - anchor;
}
#endif

/*! ZSTD_ldm_reduceTable() :
 *  reduce table indexes by `reducerValue` */
static void ZSTD_ldm_reduceTable(ldmEntry_t* const table, U32 const size,
                                 U32 const reducerValue)
{
    U32 u;
    for (u = 0; u < size; u++) {
        if (table[u].offset < reducerValue) table[u].offset = 0;
        else table[u].offset -= reducerValue;
    }
}

size_t ZSTD_ldm_generateSequences(
        ldmState_t* ldmState, rawSeqStore_t* sequences,
        ldmParams_t const* params, void const* src, size_t srcSize)
{
    U32 const maxDist = 1U << params->windowLog;
    BYTE const* const istart = (BYTE const*)src;
    BYTE const* const iend = istart + srcSize;
    size_t const kMaxChunkSize = 1 << 20;
    size_t const nbChunks = (srcSize / kMaxChunkSize) + ((srcSize % kMaxChunkSize) != 0);
    size_t chunk;
    size_t leftoverSize = 0;

    assert(ZSTD_CHUNKSIZE_MAX >= kMaxChunkSize);
    /* Check that ZSTD_window_update() has been called for this chunk prior
     * to passing it to this function.
     */
    assert(ldmState->window.nextSrc >= (BYTE const*)src + srcSize);
    /* The input could be very large (in zstdmt), so it must be broken up into
     * chunks to enforce the maximum distance and handle overflow correction.
     */
    assert(sequences->pos <= sequences->size);
    assert(sequences->size <= sequences->capacity);
    for (chunk = 0; chunk < nbChunks && sequences->size < sequences->capacity; ++chunk) {
        BYTE const* const chunkStart = istart + chunk * kMaxChunkSize;
        size_t const remaining = (size_t)(iend - chunkStart);
        BYTE const *const chunkEnd =
            (remaining < kMaxChunkSize) ? iend : chunkStart + kMaxChunkSize;
        size_t const chunkSize = chunkEnd - chunkStart;
        size_t newLeftoverSize;
        size_t const prevSize = sequences->size;

        assert(chunkStart < iend);
        /* 1. Perform overflow correction if necessary. */
        if (ZSTD_window_needOverflowCorrection(ldmState->window, chunkEnd)) {
            U32 const ldmHSize = 1U << params->hashLog;
            U32 const correction = ZSTD_window_correctOverflow(
                &ldmState->window, /* cycleLog */ 0, maxDist, chunkStart);
            ZSTD_ldm_reduceTable(ldmState->hashTable, ldmHSize, correction);
            /* invalidate dictionaries on overflow correction */
            ldmState->loadedDictEnd = 0;
        }
        /* 2. We enforce the maximum offset allowed.
         *
         * kMaxChunkSize should be small enough that we don't lose too much of
         * the window through early invalidation.
         * TODO: * Test the chunk size.
         *       * Try invalidation after the sequence generation and test the
         *         the offset against maxDist directly.
         *
         * NOTE: Because of dictionaries + sequence splitting we MUST make sure
         * that any offset used is valid at the END of the sequence, since it may
         * be split into two sequences. This condition holds when using
         * ZSTD_window_enforceMaxDist(), but if we move to checking offsets
         * against maxDist directly, we'll have to carefully handle that case.
         */
        ZSTD_window_enforceMaxDist(&ldmState->window, chunkEnd, maxDist, &ldmState->loadedDictEnd, NULL);
        /* 3. Generate the sequences for the chunk, and get newLeftoverSize. */
        newLeftoverSize = ZSTD_ldm_generateSequences_internal(
            ldmState, sequences, params, chunkStart, chunkSize);
        if (ZSTD_isError(newLeftoverSize))
            return newLeftoverSize;
        /* 4. We add the leftover literals from previous iterations to the first
         *    newly generated sequence, or add the `newLeftoverSize` if none are
         *    generated.
         */
        /* Prepend the leftover literals from the last call */
        if (prevSize < sequences->size) {
            sequences->seq[prevSize].litLength += (U32)leftoverSize;
            leftoverSize = newLeftoverSize;
        } else {
            assert(newLeftoverSize == chunkSize);
            leftoverSize += chunkSize;
        }
    }
    return 0;
}

void ZSTD_ldm_skipSequences(rawSeqStore_t* rawSeqStore, size_t srcSize, U32 const minMatch) {
    while (srcSize > 0 && rawSeqStore->pos < rawSeqStore->size) {
        rawSeq* seq = rawSeqStore->seq + rawSeqStore->pos;
        if (srcSize <= seq->litLength) {
            /* Skip past srcSize literals */
            seq->litLength -= (U32)srcSize;
            return;
        }
        srcSize -= seq->litLength;
        seq->litLength = 0;
        if (srcSize < seq->matchLength) {
            /* Skip past the first srcSize of the match */
            seq->matchLength -= (U32)srcSize;
            if (seq->matchLength < minMatch) {
                /* The match is too short, omit it */
                if (rawSeqStore->pos + 1 < rawSeqStore->size) {
                    seq[1].litLength += seq[0].matchLength;
                }
                rawSeqStore->pos++;
            }
            return;
        }
        srcSize -= seq->matchLength;
        seq->matchLength = 0;
        rawSeqStore->pos++;
    }
}

/**
 * If the sequence length is longer than remaining then the sequence is split
 * between this block and the next.
 *
 * Returns the current sequence to handle, or if the rest of the block should
 * be literals, it returns a sequence with offset == 0.
 */
static rawSeq maybeSplitSequence(rawSeqStore_t* rawSeqStore,
                                 U32 const remaining, U32 const minMatch)
{
    rawSeq sequence = rawSeqStore->seq[rawSeqStore->pos];
    assert(sequence.offset > 0);
    /* Likely: No partial sequence */
    if (remaining >= sequence.litLength + sequence.matchLength) {
        rawSeqStore->pos++;
        return sequence;
    }
    /* Cut the sequence short (offset == 0 ==> rest is literals). */
    if (remaining <= sequence.litLength) {
        sequence.offset = 0;
    } else if (remaining < sequence.litLength + sequence.matchLength) {
        sequence.matchLength = remaining - sequence.litLength;
        if (sequence.matchLength < minMatch) {
            sequence.offset = 0;
        }
    }
    /* Skip past `remaining` bytes for the future sequences. */
    ZSTD_ldm_skipSequences(rawSeqStore, remaining, minMatch);
    return sequence;
}

void ZSTD_ldm_skipRawSeqStoreBytes(rawSeqStore_t* rawSeqStore, size_t nbBytes) {
    U32 currPos = (U32)(rawSeqStore->posInSequence + nbBytes);
    while (currPos && rawSeqStore->pos < rawSeqStore->size) {
        rawSeq currSeq = rawSeqStore->seq[rawSeqStore->pos];
        if (currPos >= currSeq.litLength + currSeq.matchLength) {
            currPos -= currSeq.litLength + currSeq.matchLength;
            rawSeqStore->pos++;
        } else {
            rawSeqStore->posInSequence = currPos;
            break;
        }
    }
    if (currPos == 0 || rawSeqStore->pos == rawSeqStore->size) {
        rawSeqStore->posInSequence = 0;
    }
}

size_t ZSTD_ldm_blockCompress(rawSeqStore_t* rawSeqStore,
    ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM],
    void const* src, size_t srcSize)
{
    const ZSTD_compressionParameters* const cParams = &ms->cParams;
    unsigned const minMatch = cParams->minMatch;
    ZSTD_blockCompressor const blockCompressor =
        ZSTD_selectBlockCompressor(cParams->strategy, ZSTD_matchState_dictMode(ms));
    /* Input bounds */
    BYTE const* const istart = (BYTE const*)src;
    BYTE const* const iend = istart + srcSize;
    /* Input positions */
    BYTE const* ip = istart;

    DEBUGLOG(5, "ZSTD_ldm_blockCompress: srcSize=%zu", srcSize);
    /* If using opt parser, use LDMs only as candidates rather than always accepting them */
    if (cParams->strategy >= ZSTD_btopt) {
        size_t lastLLSize;
        ms->ldmSeqStore = rawSeqStore;
        lastLLSize = blockCompressor(ms, seqStore, rep, src, srcSize);
        ZSTD_ldm_skipRawSeqStoreBytes(rawSeqStore, srcSize);
        return lastLLSize;
    }

    assert(rawSeqStore->pos <= rawSeqStore->size);
    assert(rawSeqStore->size <= rawSeqStore->capacity);
    /* Loop through each sequence and apply the block compressor to the literals */
    while (rawSeqStore->pos < rawSeqStore->size && ip < iend) {
        /* maybeSplitSequence updates rawSeqStore->pos */
        rawSeq const sequence = maybeSplitSequence(rawSeqStore,
                                                   (U32)(iend - ip), minMatch);
        int i;
        /* End signal */
        if (sequence.offset == 0)
            break;

        assert(ip + sequence.litLength + sequence.matchLength <= iend);

        /* Fill tables for block compressor */
        ZSTD_ldm_limitTableUpdate(ms, ip);
        ZSTD_ldm_fillFastTables(ms, ip);
        /* Run the block compressor */
        DEBUGLOG(5, "pos %u : calling block compressor on segment of size %u", (unsigned)(ip-istart), sequence.litLength);
        {
            size_t const newLitLength =
                blockCompressor(ms, seqStore, rep, ip, sequence.litLength);
            ip += sequence.litLength;
            /* Update the repcodes */
            for (i = ZSTD_REP_NUM - 1; i > 0; i--)
                rep[i] = rep[i-1];
            rep[0] = sequence.offset;
            /* Store the sequence */
            ZSTD_storeSeq(seqStore, newLitLength, ip - newLitLength, iend,
                          sequence.offset + ZSTD_REP_MOVE,
                          sequence.matchLength - MINMATCH);
            ip += sequence.matchLength;
        }
    }
    /* Fill the tables for the block compressor */
    ZSTD_ldm_limitTableUpdate(ms, ip);
    ZSTD_ldm_fillFastTables(ms, ip);
    /* Compress the last literals */
    return blockCompressor(ms, seqStore, rep, ip, iend - ip);
}
