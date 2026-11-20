/* Shared helpers for the file-list microbenchmarks. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static char **kb_read_list(const char *path, size_t *count) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        perror(path);
        exit(2);
    }
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)size + 1);
    if (fread(buf, 1, (size_t)size, f) != (size_t)size) {
        perror("fread");
        exit(2);
    }
    buf[size] = '\0';
    fclose(f);

    size_t cap = 1024, n = 0;
    char **list = malloc(cap * sizeof(char *));
    for (char *line = strtok(buf, "\n"); line; line = strtok(NULL, "\n")) {
        if (n == cap) {
            cap *= 2;
            list = realloc(list, cap * sizeof(char *));
        }
        list[n++] = line;
    }
    if (n == 0) {
        fprintf(stderr, "%s: empty file list\n", path);
        exit(2);
    }
    *count = n;
    return list;
}

static long long kb_now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (long long)t.tv_sec * 1000000000LL + t.tv_nsec;
}

static void kb_report(const char *op, long ops, size_t files, long errors, long long ns) {
    printf("KBLOOP op=%s ops=%ld files=%zu errors=%ld ns_total=%lld ns_per_op=%.2f\n",
           op, ops, files, errors, ns, (double)ns / (double)ops);
}
