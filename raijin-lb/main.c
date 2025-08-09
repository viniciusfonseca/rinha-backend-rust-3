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

/* forward */
struct conn;

/* wrapper for sqe user data */
struct io_data {
    enum op_type type;
    struct conn *c;       // connection pointer
    int fd;               // fd involved in the op
    int buf_id;           // 0 = client_buf, 1 = backend_buf
    off_t offset;         // for partial writes
    ssize_t total_len;    // for writes: total bytes we intended to write
};

struct conn {
    int client_fd;
    int backend_fd;

    int client_closed;   // client sent EOF
    int backend_closed;  // backend sent EOF

    int pending_ops;     // outstanding io_uring ops for this connection

    char client_buf[BUF_SIZE];
    char backend_buf[BUF_SIZE];
};

/* global */
static struct io_uring ring;
static int server_fd = -1;
static volatile int stop = 0;
static int backend_rr = 0;

static void on_sigint(int s) {
    (void)s;
    stop = 1;
}

/* utility */
static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void close_connection(struct conn *c) {
    if (!c) return;
    if (c->client_fd >= 0) { close(c->client_fd); c->client_fd = -1; }
    if (c->backend_fd >= 0) { close(c->backend_fd); c->backend_fd = -1; }
    free(c);
}

/* Only free when both sides closed and no pending ops */
static void maybe_free_conn(struct conn *c) {
    if (!c) return;
    if (c->client_closed && c->backend_closed && c->pending_ops == 0) {
        close_connection(c);
    }
}

/* create & bind TCP listening socket */
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

/* helpers to prepare io_data and set user_data on sqe */
static struct io_data* make_io_data(enum op_type t, struct conn *c, int fd, int buf_id, off_t offset, ssize_t total_len) {
    struct io_data *d = malloc(sizeof(*d));
    if (!d) { perror("malloc io_data"); return NULL; }
    d->type = t;
    d->c = c;
    d->fd = fd;
    d->buf_id = buf_id;
    d->offset = offset;
    d->total_len = total_len;
    return d;
}

/* submit accept - always keep one outstanding accept */
static int submit_accept() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;
    struct io_data *d = make_io_data(OP_ACCEPT, NULL, server_fd, 0, 0, 0);
    if (!d) return -1;
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
    if (!sqe) { close(sfd); return -1; }

    io_uring_prep_connect(sqe, sfd, (struct sockaddr *)&sun, sizeof(sun));
    struct io_data *d = make_io_data(OP_CONNECT, c, sfd, 0, 0, 0);
    if (!d) { close(sfd); return -1; }

    io_uring_sqe_set_data(sqe, d);
    c->pending_ops++;
    return io_uring_submit(&ring);
}

/* submit a read on fd into buffer buf_id */
static int submit_read(struct conn *c, int fd, int buf_id) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;

    char *buf = (buf_id == 0) ? c->client_buf : c->backend_buf;
    io_uring_prep_read(sqe, fd, buf, BUF_SIZE, 0);
    struct io_data *d = make_io_data(OP_READ, c, fd, buf_id, 0, 0);
    if (!d) return -1;

    io_uring_sqe_set_data(sqe, d);
    c->pending_ops++;
    return io_uring_submit(&ring);
}

/* submit a write on fd using buffer buf_id, offset and length */
static int submit_write(struct conn *c, int fd, int buf_id, off_t offset, ssize_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return -1;

    char *buf = (buf_id == 0) ? c->client_buf : c->backend_buf;
    io_uring_prep_write(sqe, fd, buf + offset, len, 0);
    struct io_data *d = make_io_data(OP_WRITE, c, fd, buf_id, offset, len);
    if (!d) return -1;

    io_uring_sqe_set_data(sqe, d);
    c->pending_ops++;
    return io_uring_submit(&ring);
}

/* handle a single CQE */
static void handle_cqe(struct io_uring_cqe *cqe) {
    struct io_data *d = io_uring_cqe_get_data(cqe);
    if (!d) {
        io_uring_cqe_seen(&ring, cqe);
        return;
    }

    int res = cqe->res;

    if (d->type == OP_ACCEPT) {
        /* Accept finished */
        if (res >= 0) {
            int client_fd = res;
            set_nonblock(client_fd);

            struct conn *c = calloc(1, sizeof(*c));
            if (!c) {
                perror("calloc conn");
                close(client_fd);
            } else {
                c->client_fd = client_fd;
                c->backend_fd = -1;
                c->client_closed = 0;
                c->backend_closed = 0;
                c->pending_ops = 0;

                const char *backend_path = backends[(backend_rr++) % NUM_BACKENDS];
                if (submit_connect_backend(c, backend_path) < 0) {
                    perror("submit_connect_backend");
                    close_connection(c);
                }
            }
        } else {
            if (res != -EAGAIN && res != -EINTR) {
                fprintf(stderr, "accept failed: %s\n", strerror(-res));
            }
        }

        /* always reissue accept */
        submit_accept();
    }
    else if (d->type == OP_CONNECT) {
        struct conn *c = d->c;
        int bfd = d->fd;
        /* connect result in res */
        if (res == 0) {
            /* connected */
            c->backend_fd = bfd;
            set_nonblock(bfd);

            /* start reading both sides */
            if (submit_read(c, c->client_fd, 0) < 0) {
                perror("submit_read client");
                close_connection(c);
            }
            if (submit_read(c, c->backend_fd, 1) < 0) {
                perror("submit_read backend");
                close_connection(c);
            }
        } else {
            fprintf(stderr, "connect to backend failed: %s\n", strerror(-res));
            close(bfd);
            close_connection(c);
        }
    }
    else if (d->type == OP_READ) {
        struct conn *c = d->c;
        int fd = d->fd;
        int buf_id = d->buf_id;

        /* read finished: res = bytes read or <=0 */
        c->pending_ops--;
        if (res <= 0) {
            if (res == 0) {
                /* peer closed (EOF) */
                if (buf_id == 0) {
                    c->client_closed = 1;
                    if (c->backend_fd >= 0) shutdown(c->backend_fd, SHUT_WR);
                } else {
                    c->backend_closed = 1;
                    if (c->client_fd >= 0) shutdown(c->client_fd, SHUT_WR);
                }
            } else {
                /* read error */
                //fprintf(stderr, "read error fd %d: %s\n", fd, strerror(-res));
                if (buf_id == 0) c->client_closed = 1;
                else c->backend_closed = 1;
                if (buf_id == 0 && c->backend_fd >= 0) shutdown(c->backend_fd, SHUT_WR);
                if (buf_id == 1 && c->client_fd >= 0) shutdown(c->client_fd, SHUT_WR);
            }
            maybe_free_conn(c);
        } else {
            /* got data -> submit write to peer */
            ssize_t got = res;
            if (buf_id == 0) {
                /* client -> backend */
                if (c->backend_fd >= 0 && !c->backend_closed) {
                    if (submit_write(c, c->backend_fd, 0, 0, got) < 0) {
                        perror("submit_write backend");
                        c->backend_closed = 1;
                        maybe_free_conn(c);
                    }
                } else {
                    // backend not available -> drop connection
                    c->backend_closed = 1;
                    maybe_free_conn(c);
                }
            } else {
                /* backend -> client */
                if (c->client_fd >= 0 && !c->client_closed) {
                    if (submit_write(c, c->client_fd, 1, 0, got) < 0) {
                        perror("submit_write client");
                        c->client_closed = 1;
                        maybe_free_conn(c);
                    }
                } else {
                    c->client_closed = 1;
                    maybe_free_conn(c);
                }
            }
        }
    }
    else if (d->type == OP_WRITE) {
        struct conn *c = d->c;
        int fd = d->fd;
        int buf_id = d->buf_id;
        ssize_t wrote = res;
        /* write finished (possibly partial) */
        c->pending_ops--;
        if (res < 0) {
            //fprintf(stderr, "write error fd %d: %s\n", fd, strerror(-res));
            /* on write error consider that side closed for further IO */
            if (buf_id == 0) c->backend_closed = 1;
            else c->client_closed = 1;
            maybe_free_conn(c);
        } else {
            off_t off = d->offset;
            ssize_t total = d->total_len;
            off += wrote;
            if (off < total) {
                /* partial write: submit remaining */
                if (submit_write(c, fd, buf_id, off, total - off) < 0) {
                    perror("submit_write partial");
                    if (buf_id == 0) c->backend_closed = 1;
                    else c->client_closed = 1;
                    maybe_free_conn(c);
                }
            } else {
                /* full write completed -> re-issue read on source side if not closed */
                if (buf_id == 0) {
                    /* we wrote client_buf to backend, so re-read from client */
                    if (!c->client_closed && c->client_fd >= 0) {
                        if (submit_read(c, c->client_fd, 0) < 0) {
                            perror("submit_read client");
                            c->client_closed = 1;
                        }
                    }
                } else {
                    /* we wrote backend_buf to client, re-read from backend */
                    if (!c->backend_closed && c->backend_fd >= 0) {
                        if (submit_read(c, c->backend_fd, 1) < 0) {
                            perror("submit_read backend");
                            c->backend_closed = 1;
                        }
                    }
                }
                maybe_free_conn(c);
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

    server_fd = make_server_socket(LISTEN_PORT);
    if (server_fd < 0) return 1;

    if (io_uring_queue_init(RING_ENTRIES, &ring, 0) < 0) {
        perror("io_uring_queue_init");
        close(server_fd);
        return 1;
    }

    if (submit_accept() < 0) {
        fprintf(stderr, "submit_accept failed\n");
        io_uring_queue_exit(&ring);
        close(server_fd);
        return 1;
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
