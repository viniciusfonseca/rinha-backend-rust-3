// gcc -O2 -Wall -o lb_io_uring lb_io_uring.c -luring
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <liburing.h>

#define LISTEN_PORT 9999
#define BACKLOG 512
#define RING_ENTRIES 1024
#define BUF_SIZE 8192
#define NUM_BACKENDS 2

const char *backends[NUM_BACKENDS] = {
    "/tmp/sockets/api01.sock",
    "/tmp/sockets/api02.sock",
};

enum op_type {
    OP_ACCEPT,
    OP_CONNECT,
    OP_READ,
    OP_WRITE
};

/* wrapper for sqe user data */
struct io_data {
    enum op_type type;
    void *conn;           // points to struct conn *
    int fd;               // fd involved in the op
    int buf_id;           // 0 = client_buf, 1 = backend_buf
    ssize_t len;          // used for write length or read result
};

/* per-connection structure */
struct conn {
    int client_fd;
    int backend_fd;
    int closing;          // flag set when either side closed
    char client_buf[BUF_SIZE];
    char backend_buf[BUF_SIZE];
};

static struct io_uring ring;
static int server_fd = -1;
static volatile int stop = 0;
static int backend_rr = 0;

static void on_sigint(int s) {
    (void)s;
    stop = 1;
}

/* set non-blocking */
static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* create and bind the TCP listening socket */
static int make_server_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) { perror("socket"); return -1; }

    int on = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, BACKLOG) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

/* helper to allocate io_data and set as user_data on sqe */
static struct io_data* prepare_io(struct io_uring_sqe *sqe, enum op_type t, struct conn *c, int fd, int buf_id, off_t offset) {
    struct io_data *d = malloc(sizeof(*d));
    if (!d) { perror("malloc io_data"); return NULL; }
    d->type = t;
    d->conn = c;
    d->fd = fd;
    d->buf_id = buf_id;
    d->len = 0;
    io_uring_sqe_set_data(sqe, d);
    (void) offset;
    return d;
}

/* submit accept operation (always keep one outstanding) */
static int submit_accept() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;
    struct io_data *d = malloc(sizeof(*d));
    if (!d) return -1;
    d->type = OP_ACCEPT;
    d->conn = NULL;
    d->fd = server_fd;
    d->buf_id = 0;
    d->len = 0;
    io_uring_prep_accept(sqe, server_fd, NULL, NULL, SOCK_NONBLOCK);
    io_uring_sqe_set_data(sqe, d);
    return io_uring_submit(&ring);
}

/* submit connect to a UNIX domain socket (backend) */
static int submit_connect_backend(struct conn *c, const char *path) {
    int sfd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sfd < 0) {
        perror("socket AF_UNIX");
        return -1;
    }

    struct sockaddr_un sun;
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    strncpy(sun.sun_path, path, sizeof(sun.sun_path)-1);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        close(sfd);
        return -1;
    }

    /* prepare connect */
    io_uring_prep_connect(sqe, sfd, (struct sockaddr *)&sun, sizeof(sun));
    struct io_data *d = prepare_io(sqe, OP_CONNECT, c, sfd, 0, 0);
    if (!d) { close(sfd); return -1; }

    return io_uring_submit(&ring);
}

/* submit a read on given fd into the chosen buffer */
static int submit_read(struct conn *c, int fd, int buf_id) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;

    char *buf = (buf_id == 0) ? c->client_buf : c->backend_buf;
    io_uring_prep_read(sqe, fd, buf, BUF_SIZE, 0);
    struct io_data *d = prepare_io(sqe, OP_READ, c, fd, buf_id, 0);
    if (!d) return -1;
    return io_uring_submit(&ring);
}

/* submit a write on given fd from the chosen buffer with length len */
static int submit_write(struct conn *c, int fd, int buf_id, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;

    char *buf = (buf_id == 0) ? c->client_buf : c->backend_buf;
    io_uring_prep_write(sqe, fd, buf, len, 0);
    struct io_data *d = prepare_io(sqe, OP_WRITE, c, fd, buf_id, 0);
    if (!d) return -1;
    d->len = (ssize_t)len;
    return io_uring_submit(&ring);
}

/* close and free connection */
static void close_connection(struct conn *c) {
    if (!c) return;
    if (c->client_fd >= 0) { close(c->client_fd); c->client_fd = -1; }
    if (c->backend_fd >= 0) { close(c->backend_fd); c->backend_fd = -1; }
    free(c);
}

/* main CQE handling */
static void handle_cqe(struct io_uring_cqe *cqe) {
    struct io_data *d = io_uring_cqe_get_data(cqe);
    if (!d) {
        // should not happen
        io_uring_cqe_seen(&ring, cqe);
        return;
    }

    int res = cqe->res;

    if (d->type == OP_ACCEPT) {
        /* accept completed: res is client fd or negative on error */
        if (res >= 0) {
            int client_fd = res;
            set_nonblock(client_fd);

            // allocate connection container
            struct conn *c = calloc(1, sizeof(*c));
            if (!c) { perror("calloc conn"); close(client_fd); }
            else {
                c->client_fd = client_fd;
                c->backend_fd = -1;
                c->closing = 0;

                // choose backend path round-robin
                const char *backend_path = backends[(backend_rr++) % NUM_BACKENDS];

                // submit connect. When connect completes we'll start reads.
                if (submit_connect_backend(c, backend_path) < 0) {
                    perror("submit_connect_backend");
                    close_connection(c);
                }
            }
        } else {
            // accept error: log and continue
            if (res != -EAGAIN && res != -EINTR) {
                fprintf(stderr, "accept failed: %s\n", strerror(-res));
            }
        }

        // always submit another accept to keep accepting connections
        submit_accept();
    }
    else if (d->type == OP_CONNECT) {
        /* connect completed: d->fd is backend socket */
        struct conn *c = d->conn;
        int bfd = d->fd;

        if (res == 0) {
            // connected
            c->backend_fd = bfd;
            set_nonblock(bfd);

            // Start reading both sides
            if (submit_read(c, c->client_fd, 0) < 0) { perror("submit_read client"); close_connection(c); }
            if (submit_read(c, c->backend_fd, 1) < 0) { perror("submit_read backend"); close_connection(c); }
        } else {
            // connect failed
            fprintf(stderr, "connect to backend failed: %s\n", strerror(-res));
            close(bfd);
            close_connection(c);
        }
    }
    else if (d->type == OP_READ) {
        struct conn *c = d->conn;
        int fd = d->fd;
        int buf_id = d->buf_id;

        if (res <= 0) {
            // EOF or error: close both sides
            if (res == 0) {
                //peer closed
            } else {
                // read error
                fprintf(stderr, "read error on fd %d: %s\n", fd, strerror(-res));
            }
            close_connection(c);
        } else {
            // got data -> write to peer
            ssize_t got = res;
            if (buf_id == 0) {
                // read from client -> write to backend
                if (c->backend_fd >= 0) {
                    if (submit_write(c, c->backend_fd, 0, (size_t)got) < 0) {
                        perror("submit_write backend");
                        close_connection(c);
                    }
                } else {
                    // backend not ready
                    close_connection(c);
                }
            } else {
                // read from backend -> write to client
                if (c->client_fd >= 0) {
                    if (submit_write(c, c->client_fd, 1, (size_t)got) < 0) {
                        perror("submit_write client");
                        close_connection(c);
                    }
                } else {
                    close_connection(c);
                }
            }
        }
    }
    else if (d->type == OP_WRITE) {
        struct conn *c = d->conn;
        int fd = d->fd;
        int buf_id = d->buf_id;

        if (res < 0) {
            // write error
            fprintf(stderr, "write error on fd %d: %s\n", fd, strerror(-res));
            close_connection(c);
        } else {
            // after a write finishes, re-issue a read on the source side
            if (buf_id == 0) {
                // we wrote client_buf to backend -> read from client again
                if (c->client_fd >= 0)
                    submit_read(c, c->client_fd, 0);
                else close_connection(c);
            } else {
                // we wrote backend_buf to client -> read from backend again
                if (c->backend_fd >= 0)
                    submit_read(c, c->backend_fd, 1);
                else close_connection(c);
            }
        }
    }

    free(d);
    io_uring_cqe_seen(&ring, cqe);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    printf("Starting raijin-lb\n");
    server_fd = make_server_socket(LISTEN_PORT);
    if (server_fd < 0) return 1;

    int queue_init_result = io_uring_queue_init(RING_ENTRIES, &ring, 0);
    if (queue_init_result < 0) {
        printf("io_uring_queue_init failed - %s - %d\n", strerror(-queue_init_result), -queue_init_result);
        perror("io_uring_queue_init");
        close(server_fd);
        return 2;
    }

    // Prime a single accept
    if (submit_accept() < 0) {
        fprintf(stderr, "submit_accept failed\n");
        io_uring_queue_exit(&ring);
        close(server_fd);
        return 3;
    }

    printf("Listening on port %d\n", LISTEN_PORT);

    while (!stop) {
        struct io_uring_cqe *cqe = NULL;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("io_uring_wait_cqe");
            break;
        }
        if (cqe)
            handle_cqe(cqe);
    }

    printf("Shutting down\n");
    io_uring_queue_exit(&ring);
    if (server_fd >= 0) close(server_fd);
    return 0;
}
