/*
 * uffd-handler: page fault handler for Firecracker snapshot restore.
 *
 * Receives a userfaultfd from Firecracker via UDS, then:
 *   1. mmaps the snapshot mem file with MAP_POPULATE (pre-warm)
 *   2. enters the fault-handling event loop immediately
 *   3. spawns a prefetch thread that eagerly installs remaining pages
 *
 * The main thread serves on-demand faults with zero delay. The prefetch
 * thread races ahead installing pages the guest hasn't touched yet.
 * By the time the guest runs node -v, most pages are already installed.
 *
 * Usage: uffd-handler <uds-path> <mem-file-path>
 *
 * Compile: gcc -static -O2 -pthread -o uffd-handler uffd-handler.c
 */

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <linux/userfaultfd.h>

#define MAX_REGIONS 4
#define MAX_JSON    8192

struct region {
    uint64_t base_addr;
    uint64_t size;
    uint64_t offset;
    uint64_t page_size;
};

struct handler_state {
    int                uffd_fd;
    void              *backing;
    size_t             backing_size;
    struct region      regions[MAX_REGIONS];
    int                num_regions;
    uint64_t           total_pages;
    /* Bitmap: 1 bit per page. Set when installed (on-demand or prefetch). */
    atomic_char       *installed;
};

static int parse_regions(const char *json, int len, struct region *out, int max) {
    int count = 0;
    const char *p = json;
    const char *end = json + len;

    while (p < end && count < max) {
        const char *obj = memchr(p, '{', end - p);
        if (!obj) break;

        struct region r = {0};
        r.page_size = 4096;

        const char *obj_end = memchr(obj, '}', end - obj);
        if (!obj_end) break;

        const char *f;
        if ((f = strstr(obj, "\"base_host_virt_addr\"")) && f < obj_end)
            r.base_addr = strtoull(strchr(f + 20, ':') + 1, NULL, 10);
        if ((f = strstr(obj, "\"size\"")) && f < obj_end)
            r.size = strtoull(strchr(f + 6, ':') + 1, NULL, 10);
        if ((f = strstr(obj, "\"offset\"")) && f < obj_end)
            r.offset = strtoull(strchr(f + 8, ':') + 1, NULL, 10);
        if ((f = strstr(obj, "\"page_size\"")) && f < obj_end) {
            const char *after = f + 11;
            if (*after == '"' || *after == ' ' || *after == ':')
                r.page_size = strtoull(strchr(f + 11, ':') + 1, NULL, 10);
        }

        out[count++] = r;
        p = obj_end + 1;
    }
    return count;
}

static uint64_t page_global_index(struct handler_state *s, int region, uint64_t offset_in_region) {
    uint64_t idx = 0;
    for (int i = 0; i < region; i++)
        idx += s->regions[i].size / s->regions[i].page_size;
    idx += offset_in_region / s->regions[region].page_size;
    return idx;
}

/* Returns 1 if already set. */
static int mark_installed(struct handler_state *s, uint64_t idx) {
    uint64_t byte = idx / 8;
    char bit = 1 << (idx % 8);
    char old = atomic_fetch_or(&s->installed[byte], bit);
    return (old & bit) != 0;
}

/* Prefetch thread: install all pages that haven't been served on-demand. */
static void *prefetch_thread(void *arg) {
    struct handler_state *s = (struct handler_state *)arg;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint64_t installed = 0, skipped = 0;
    for (int r = 0; r < s->num_regions; r++) {
        uint64_t psize = s->regions[r].page_size;
        uint64_t npages = s->regions[r].size / psize;

        for (uint64_t p = 0; p < npages; p++) {
            uint64_t idx = page_global_index(s, r, p * psize);
            if (mark_installed(s, idx)) {
                skipped++;
                continue;
            }

            uint64_t dst = s->regions[r].base_addr + p * psize;
            void *src = (char *)s->backing + s->regions[r].offset + p * psize;

            struct uffdio_copy copy = {
                .dst  = dst,
                .src  = (uint64_t)src,
                .len  = psize,
                .mode = UFFDIO_COPY_MODE_DONTWAKE,
            };
            if (ioctl(s->uffd_fd, UFFDIO_COPY, &copy) != -1)
                installed++;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    long ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;
    fprintf(stderr, "uffd-handler: prefetched %lu pages, %lu on-demand, in %ldms\n",
            (unsigned long)installed, (unsigned long)skipped, ms);
    return NULL;
}

static int handshake(const char *uds_path, int *uffd_out, char *json_out, int *json_len) {
    int srv = socket(AF_UNIX, SOCK_STREAM, 0);
    if (srv < 0) return -1;

    struct sockaddr_un addr = {.sun_family = AF_UNIX};
    strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path) - 1);
    unlink(uds_path);

    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(srv);
        return -1;
    }
    if (listen(srv, 1) < 0) {
        close(srv);
        return -1;
    }

    char ready_path[256];
    snprintf(ready_path, sizeof(ready_path), "%s.ready", uds_path);
    int rfd = open(ready_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (rfd >= 0) close(rfd);

    int stream = accept(srv, NULL, NULL);
    close(srv);
    if (stream < 0) return -1;

    struct msghdr msg = {0};
    struct iovec iov;
    char cmsg_buf[CMSG_SPACE(sizeof(int))];

    iov.iov_base = json_out;
    iov.iov_len = MAX_JSON;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    ssize_t n = recvmsg(stream, &msg, 0);
    close(stream);
    if (n <= 0) return -1;

    *json_len = (int)n;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg || cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS)
        return -1;

    memcpy(uffd_out, CMSG_DATA(cmsg), sizeof(int));
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: uffd-handler <uds-path> <mem-file>\n");
        return 1;
    }
    const char *uds_path = argv[1];
    const char *mem_path = argv[2];

    int mem_fd = open(mem_path, O_RDONLY);
    if (mem_fd < 0) { perror("open mem file"); return 1; }
    struct stat st;
    fstat(mem_fd, &st);
    size_t mem_size = st.st_size;

    void *backing = mmap(NULL, mem_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, mem_fd, 0);
    close(mem_fd);
    if (backing == MAP_FAILED) { perror("mmap mem file"); return 1; }

    int uffd_fd;
    char json[MAX_JSON];
    int json_len;

    if (handshake(uds_path, &uffd_fd, json, &json_len) < 0) {
        fprintf(stderr, "handshake failed\n");
        return 1;
    }

    struct handler_state state = {
        .uffd_fd      = uffd_fd,
        .backing      = backing,
        .backing_size = mem_size,
    };
    state.num_regions = parse_regions(json, json_len, state.regions, MAX_REGIONS);
    if (state.num_regions == 0) {
        fprintf(stderr, "no regions parsed\n");
        return 1;
    }

    state.total_pages = 0;
    for (int i = 0; i < state.num_regions; i++)
        state.total_pages += state.regions[i].size / state.regions[i].page_size;

    size_t bitmap_bytes = (state.total_pages + 7) / 8;
    state.installed = calloc(bitmap_bytes, 1);

    fprintf(stderr, "uffd-handler: %d regions, %lu pages, mem=%zuMB\n",
            state.num_regions, (unsigned long)state.total_pages, mem_size / (1024*1024));

    /* Start prefetch in background — main thread handles faults immediately. */
    pthread_t prefetch;
    pthread_create(&prefetch, NULL, prefetch_thread, &state);

    /* Event loop: serve on-demand faults + balloon events. */
    struct pollfd pfd = {.fd = uffd_fd, .events = POLLIN};

    for (;;) {
        int ret = poll(&pfd, 1, 5000);
        if (ret < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (ret == 0) continue;
        if (pfd.revents & (POLLERR | POLLHUP))
            break;

        struct uffd_msg msg;
        ssize_t n = read(uffd_fd, &msg, sizeof(msg));
        if (n <= 0) {
            if (errno == EAGAIN) continue;
            break;
        }

        if (msg.event == UFFD_EVENT_PAGEFAULT) {
            uint64_t fault_addr = msg.arg.pagefault.address;

            for (int r = 0; r < state.num_regions; r++) {
                uint64_t base = state.regions[r].base_addr;
                uint64_t rend = base + state.regions[r].size;
                if (fault_addr < base || fault_addr >= rend)
                    continue;

                uint64_t psize = state.regions[r].page_size;
                uint64_t page_off = (fault_addr - base) & ~(psize - 1);
                uint64_t dst = base + page_off;
                void *src = (char *)backing + state.regions[r].offset + page_off;

                /* Mark so prefetch skips it */
                uint64_t idx = page_global_index(&state, r, page_off);
                mark_installed(&state, idx);

                struct uffdio_copy copy = {
                    .dst = dst, .src = (uint64_t)src,
                    .len = psize, .mode = 0, /* wake faulting thread */
                };
                if (ioctl(state.uffd_fd, UFFDIO_COPY, &copy) == -1 && errno == EEXIST) {
                    /* Prefetch installed this page with DONTWAKE — we must
                     * explicitly wake the faulting thread. */
                    struct uffdio_range wake = { .start = dst, .len = psize };
                    ioctl(state.uffd_fd, UFFDIO_WAKE, &wake);
                }
                break;
            }
        } else if (msg.event == UFFD_EVENT_REMOVE) {
            struct uffdio_range range = {
                .start = msg.arg.remove.start,
                .len   = msg.arg.remove.end - msg.arg.remove.start,
            };
            ioctl(uffd_fd, UFFDIO_UNREGISTER, &range);
        }
    }

    pthread_join(prefetch, NULL);
    munmap(backing, mem_size);
    free(state.installed);
    close(uffd_fd);

    return 0;
}
