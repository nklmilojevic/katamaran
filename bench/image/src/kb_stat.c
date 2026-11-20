/* kb_stat <file-list> <ops>: stat() the listed paths round-robin and print ns/op. */
#include <sys/stat.h>
#include "kb_list.h"

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: kb_stat <file-list> <ops>\n");
        return 2;
    }
    size_t n;
    char **paths = kb_read_list(argv[1], &n);
    long ops = atol(argv[2]), errors = 0;
    struct stat st;
    long long start = kb_now_ns();
    for (long i = 0; i < ops; i++)
        if (stat(paths[(size_t)i % n], &st) != 0)
            errors++;
    kb_report("stat", ops, n, errors, kb_now_ns() - start);
    return errors ? 1 : 0;
}
