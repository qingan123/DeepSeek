#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* DeepSeekHashV1: the web worker's 23-round Keccak variant (rounds 1..23). */
static const uint64_t ROUND_CONSTANTS[24] = {
    0x0000000000000001ULL, 0x0000000000008082ULL,
    0x800000000000808aULL, 0x8000000080008000ULL,
    0x000000000000808bULL, 0x0000000080000001ULL,
    0x8000000080008081ULL, 0x8000000000008009ULL,
    0x000000000000008aULL, 0x0000000000000088ULL,
    0x0000000080008009ULL, 0x000000008000000aULL,
    0x000000008000808bULL, 0x800000000000008bULL,
    0x8000000000008089ULL, 0x8000000000008003ULL,
    0x8000000000008002ULL, 0x8000000000000080ULL,
    0x000000000000800aULL, 0x800000008000000aULL,
    0x8000000080008081ULL, 0x8000000000008080ULL,
    0x0000000080000001ULL, 0x8000000080008008ULL,
};

static const unsigned ROTATIONS[5][5] = {
    {0, 36, 3, 41, 18},
    {1, 44, 10, 45, 2},
    {62, 6, 43, 15, 61},
    {28, 55, 25, 21, 56},
    {27, 20, 39, 8, 14},
};

static uint8_t target_hash[32];
static char prefix[96];
static int prefix_len;
static int difficulty;
static int worker_count;
static atomic_int found;
static atomic_int answer;

static inline uint64_t rotate_left(uint64_t value, unsigned bits) {
    return bits ? (value << bits) | (value >> (64U - bits)) : value;
}

static void keccak_23(uint64_t state[25]) {
    uint64_t columns[5], deltas[5], moved[25];
    for (int round = 1; round < 24; ++round) {
        for (int x = 0; x < 5; ++x) {
            columns[x] = state[x] ^ state[x + 5] ^ state[x + 10] ^
                         state[x + 15] ^ state[x + 20];
        }
        for (int x = 0; x < 5; ++x) {
            deltas[x] = columns[(x + 4) % 5] ^ rotate_left(columns[(x + 1) % 5], 1);
        }
        for (int y = 0; y < 5; ++y) {
            for (int x = 0; x < 5; ++x) state[x + 5 * y] ^= deltas[x];
        }
        for (int y = 0; y < 5; ++y) {
            for (int x = 0; x < 5; ++x) {
                moved[y + 5 * ((2 * x + 3 * y) % 5)] =
                    rotate_left(state[x + 5 * y], ROTATIONS[x][y]);
            }
        }
        for (int y = 0; y < 5; ++y) {
            for (int x = 0; x < 5; ++x) {
                state[x + 5 * y] = moved[x + 5 * y] ^
                    ((~moved[(x + 1) % 5 + 5 * y]) & moved[(x + 2) % 5 + 5 * y]);
            }
        }
        state[0] ^= ROUND_CONSTANTS[round];
    }
}

static int uint_to_decimal(unsigned value, char *output) {
    char reversed[16];
    int length = 0;
    do {
        reversed[length++] = (char)('0' + value % 10U);
        value /= 10U;
    } while (value);
    for (int i = 0; i < length; ++i) output[i] = reversed[length - i - 1];
    return length;
}

typedef struct { int start; } worker_arg;

static void *search_range(void *opaque) {
    const int start = ((worker_arg *)opaque)->start;
    uint8_t block[136];
    uint64_t state[25];
    memset(block, 0, sizeof(block));
    memcpy(block, prefix, (size_t)prefix_len);
    block[135] = 0x80;

    for (int candidate = start;
         candidate < difficulty && !atomic_load_explicit(&found, memory_order_relaxed);
         candidate += worker_count) {
        memset(block + prefix_len, 0, 16);
        const int digits = uint_to_decimal((unsigned)candidate, (char *)block + prefix_len);
        block[prefix_len + digits] = 0x06;
        memset(state, 0, sizeof(state));
        memcpy(state, block, sizeof(block));
        keccak_23(state);
        if (memcmp(state, target_hash, sizeof(target_hash)) == 0) {
            atomic_store_explicit(&answer, candidate, memory_order_relaxed);
            atomic_store_explicit(&found, 1, memory_order_release);
            break;
        }
    }
    return NULL;
}

static int hex_value(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

int main(int argc, char **argv) {
    if (argc != 5 || strlen(argv[1]) != 64) {
        fprintf(stderr, "usage: dspow_native TARGET_HEX PREFIX DIFFICULTY WORKERS\n");
        return 2;
    }
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
    fprintf(stderr, "only little-endian hosts are supported\n");
    return 2;
#endif
    for (int i = 0; i < 32; ++i) {
        const int high = hex_value(argv[1][2 * i]);
        const int low = hex_value(argv[1][2 * i + 1]);
        if (high < 0 || low < 0) return 2;
        target_hash[i] = (uint8_t)((high << 4) | low);
    }
    prefix_len = (int)strlen(argv[2]);
    if (prefix_len <= 0 || prefix_len > 80) return 2;
    memcpy(prefix, argv[2], (size_t)prefix_len);
    difficulty = atoi(argv[3]);
    worker_count = atoi(argv[4]);
    if (difficulty <= 0 || worker_count < 1 || worker_count > 32) return 2;

    pthread_t threads[32];
    worker_arg args[32];
    for (int i = 0; i < worker_count; ++i) {
        args[i].start = i;
        if (pthread_create(&threads[i], NULL, search_range, &args[i]) != 0) return 4;
    }
    for (int i = 0; i < worker_count; ++i) pthread_join(threads[i], NULL);
    if (!atomic_load_explicit(&found, memory_order_acquire)) return 3;
    printf("%d\n", atomic_load_explicit(&answer, memory_order_relaxed));
    return 0;
}
