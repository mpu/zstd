#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <unistd.h>

#include "zstd_ldm.h"

typedef struct Mode Mode;

struct Mode {
	int bucketSizeLog;
	int minMatchLength;
	int windowLog;
	int hashLog;
	int hashRateLog;
};

/* some benchmarking modes */
Mode modes[] = {
	{
		.bucketSizeLog = 3,
		.minMatchLength = 64,
		.windowLog = 27,
		.hashLog = 27 - 7,             /* hash size is window size / avg chunk length */
		.hashRateLog = 27 - (27 - 7),  /* avg chunk length is 128 */
	},
	{
		.bucketSizeLog = 3,
		.minMatchLength = 64,
		.windowLog = 30,
		.hashLog = 30 - 7,
		.hashRateLog = 30 - (30 - 7),
	},
};

static void *
emalloc(size_t n)
{
	void *p;

	p = calloc(n, 1);
	assert(p);
	return p;
}

int
main(int argc, char *argv[])
{
	ldmState_t *state;
	ldmParams_t *params;
	rawSeq *seq;
	rawSeqStore_t seqStore;
	BYTE *buf[2], *cur;
	size_t bufsz, left, next, total, blockSize, windowSize, maxNbSeq;
	size_t nBuckets;
	ssize_t rd;
	Mode *mode;
	int bid;

	assert(argc >= 2);
	{
		int i;

		i = atoi(argv[1]);
		assert(0 <= i && (size_t)i < sizeof(modes) / sizeof(modes[0]));
		mode = modes + i;
	}

	/* initialize ldm params */
	params = emalloc(sizeof(*params));
	params->enableLdm = 1;
	params->hashLog = mode->hashLog;
	params->bucketSizeLog = mode->bucketSizeLog;
	params->minMatchLength = mode->minMatchLength;
	params->hashRateLog = mode->hashRateLog;
	params->windowLog = mode->windowLog;

	/* initialize ldm state */
	state = emalloc(sizeof(*state));
	ZSTD_window_init(&state->window);
	ZSTD_window_clear(&state->window);
	state->loadedDictEnd = 0;

	state->hashTable = emalloc(((size_t)1 << mode->hashLog) * sizeof(ldmEntry_t));
	nBuckets = (size_t)1 << (mode->hashLog - mode->bucketSizeLog);
	state->bucketOffsets = emalloc(nBuckets);
	state->hashPower = ZSTD_rollingHash_primePower(mode->minMatchLength);

	/* initialize sequence store */
	windowSize = (size_t)1 << mode->windowLog;
	blockSize = MIN(ZSTD_BLOCKSIZE_MAX, windowSize);
	maxNbSeq = ZSTD_ldm_getMaxNbSeq(*params, blockSize);
	seq = emalloc(maxNbSeq * sizeof(rawSeq));

	/* use two buffers, each is half the window size */
	bufsz = windowSize / 2;
	buf[0] = emalloc(bufsz);
	buf[1] = emalloc(bufsz);

	total = 0;
	for (bid = 0;; bid ^= 1) {
		cur = buf[bid];
		rd = read(0, cur, bufsz);
		if (rd <= 0)
			break;
		left = rd;
		total += left;

		ZSTD_window_update(&state->window, cur, left);
		while (left) {
			next = left;
			if (left > blockSize)
				next = blockSize;
			left -= next;

			seqStore = kNullRawSeqStore;
			seqStore.capacity = maxNbSeq;
			seqStore.seq = seq;
			assert(ZSTD_ldm_generateSequences(state, &seqStore, params, cur, next) == 0);
			cur += next;
		}
	}

	printf("processed %zd bytes\n", total);
#if 0
	printf("stats:\n");
	printf("\tmask matches: %u (%.1f%% of the input)\n",
			ldmStats.nMaskMatch,
			100. * (float)ldmStats.nMaskMatch / total);
        printf("\tno entry: %u (%.1f%% of all mask matches)\n",
                        ldmStats.nNoEntrySlow + ldmStats.nNoEntryQuick,
                        100. * (float)(ldmStats.nNoEntrySlow + ldmStats.nNoEntryQuick) /
                                ldmStats.nMaskMatch);
        printf("\tno entry paths: %.1f%% slow, %.1f%% quick\n",
                        100. * (float)ldmStats.nNoEntrySlow /
                                (ldmStats.nNoEntrySlow + ldmStats.nNoEntryQuick),
                        100. * (float)ldmStats.nNoEntryQuick /
                                (ldmStats.nNoEntrySlow + ldmStats.nNoEntryQuick));
	printf("\tchecksum mismatches: %u (%.1f%% of mask matches)\n",
			ldmStats.nChecksumMismatch,
			100. * (float)ldmStats.nChecksumMismatch /
				(ldmStats.nMaskMatch * (1U << mode->bucketSizeLog)));
	printf("\t7bits checksum mismatches: %u (%.1f%% of all mismatches)\n",
			ldmStats.nChecksumMismatch7,
			100. * (float)ldmStats.nChecksumMismatch7 /
				ldmStats.nChecksumMismatch);
	printf("\tmatches out of bounds: %u (%.1f%% of mask matches)\n",
			ldmStats.nMatchOob,
			100. * (float)ldmStats.nMatchOob /
				(ldmStats.nMaskMatch * (1U << mode->bucketSizeLog)));

        printf("\tuseful comparisons: %zd\n", ldmStats.nUsefulComparisons);
        printf("\twasted comparisons: %zd (%.1f%% of all comparisons)\n",
                        ldmStats.nWastedComparisons,
                        100. * (float)ldmStats.nWastedComparisons /
                                (ldmStats.nWastedComparisons + ldmStats.nUsefulComparisons));
#endif
	return 0;
}
