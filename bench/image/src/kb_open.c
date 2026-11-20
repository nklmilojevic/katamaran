/* kb_open <file-list> <ops>: open()/close() the listed paths round-robin and print ns/op. */
#include <fcntl.h>
#include <unistd.h>
#include "kb_list.h"

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: kb_open <file-list> <ops>\n");
        return 2;
    }
    size_t n;
    char **paths = kb_read_list(argv[1], &n);
    long ops = atol(argv[2]), errors = 0;
    long long start = kb_now_ns();
    for (long i = 0; i < ops; i++) {
        int fd = open(paths[(size_t)i % n], O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            errors++;
        else
            close(fd);
    }
    kb_report("open_close", ops, n, errors, kb_now_ns() - start);
    return errors ? 1 : 0;
}
