#include "simulation.h"

#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

enum { CAN_WRITE, CAN_READ };
typedef struct {
    sem_t sem[2];
    Message message;
} Mailbox;

static Mailbox *boxes = MAP_FAILED;
static int closing;

static void handle_signal(int signal_number)
{
    if (signal_number == SIGCHLD)
        child_changed = 1;
    else
        stop_signal = signal_number;
}

/* Короткое ожидание позволяет заметить сигнал и внезапный выход ребёнка
 * даже тогда, когда сигнал пришёл непосредственно перед ожиданием. */
static int wait_sem(sem_t *sem)
{
    while (!stop_signal && (closing || !child_changed)) {
        struct timespec deadline;
        if (clock_gettime(CLOCK_REALTIME, &deadline) == -1)
            return -1;
        deadline.tv_nsec += 100000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            ++deadline.tv_sec;
            deadline.tv_nsec -= 1000000000L;
        }
        if (sem_timedwait(sem, &deadline) == 0)
            return 0;
        if (errno != EINTR && errno != ETIMEDOUT)
            return -1;
    }
    errno = stop_signal ? EINTR : ECHILD;
    return -1;
}

int send_message(int receiver, Message message)
{
    Mailbox *box = &boxes[receiver];
    if (wait_sem(&box->sem[CAN_WRITE]) == -1)
        return -1;
    box->message = message;
    return sem_post(&box->sem[CAN_READ]);
}

int receive_message(int receiver, Message *message)
{
    Mailbox *box = &boxes[receiver];
    if (wait_sem(&box->sem[CAN_READ]) == -1)
        return -1;
    *message = box->message;
    return sem_post(&box->sem[CAN_WRITE]);
}

int main(int argc, char **argv)
{
    if (argc < 2 || argc > 3) {
        dprintf(STDERR_FILENO, "Использование: %s INPUT [LOG]\n", argv[0]);
        return 2;
    }
    struct sigaction action = { .sa_handler = handle_signal, .sa_flags = SA_NOCLDSTOP };
    if (sigemptyset(&action.sa_mask) == -1 ||
        sigaction(SIGINT, &action, NULL) == -1 ||
        sigaction(SIGTERM, &action, NULL) == -1 ||
        sigaction(SIGCHLD, &action, NULL) == -1) {
        perror("sigaction");
        return 1;
    }
    action.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &action, NULL) == -1) {
        perror("sigaction SIGPIPE");
        return 1;
    }
    if (load_input(argv[1]) == -1) {
        perror("Входные данные");
        free_model();
        return 1;
    }
    int result = 1, fd = -1, created = 0, spawned = 0, have_clock = 0;
    size_t initialized = 0;
    int actors = config.lifts + config.passengers;
    size_t count = (size_t)actors + 1, size = 0;
    pid_t *pids = NULL;
    char name[64];
    int length = snprintf(name, sizeof name, "/acos03_%ld", (long)getpid());
    if (length < 0 || (size_t)length >= sizeof name) {
        errno = EOVERFLOW;
        goto fail;
    }
    log_fd = open(argc == 3 ? argv[2] : "elevator-03.log", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (log_fd == -1)
        goto fail;
    pids = calloc((size_t)actors, sizeof *pids);
    if (!pids)
        goto fail;
    if (count > SIZE_MAX / sizeof *boxes) {
        errno = EOVERFLOW;
        goto fail;
    }
    size = count * sizeof *boxes;
    if ((off_t)size < 0 || (uintmax_t)(off_t)size != (uintmax_t)size) {
        errno = EOVERFLOW;
        goto fail;
    }
    fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd == -1)
        goto fail;
    created = 1;
    if (ftruncate(fd, (off_t)size) == -1)
        goto fail;
    boxes = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (boxes == MAP_FAILED)
        goto fail;
    int close_result = close(fd);
    fd = -1;
    if (close_result == -1)
        goto fail;
    for (; initialized < 2 * count; ++initialized) {
        sem_t *sem = &boxes[initialized / 2].sem[initialized % 2];
        if (sem_init(sem, 1, initialized % 2 == CAN_WRITE) == -1)
            goto fail;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &started) == -1)
        goto fail;
    have_clock = 1;
    for (int actor = 1; actor <= actors; ++actor) {
        if (stop_signal || child_changed) {
            errno = EINTR;
            goto fail;
        }
        pid_t pid = fork();
        if (pid == -1)
            goto fail;
        if (pid == 0) {
            child_changed = 0;
            int status = actor_main(actor);
            if (status == -1 && !stop_signal)
                perror("Участник моделирования");
            _exit(status == 0 || stop_signal ? 0 : 1);
        }
        pids[spawned++] = pid;
    }
    if (dispatcher_main() == -1)
        goto fail;
    /* После STOP нормальное завершение детей больше не является ошибкой. */
    closing = 1;
    for (int actor = 1; actor <= actors; ++actor) {
        if (send_message(actor, (Message){ STOP, 0, 0 }) == -1)
            goto fail;
    }
    result = 0;
    goto cleanup;
fail:
    if (!stop_signal)
        perror("Моделирование");
cleanup:
    if (result != 0 || stop_signal) {
        for (int i = 0; i < spawned; ++i) {
            if (kill(pids[i], SIGTERM) == -1 && errno != ESRCH)
                perror("kill");
        }
    }
    for (int i = 0; i < spawned; ++i) {
        int status;
        pid_t pid;
        do {
            pid = waitpid(pids[i], &status, 0);
        } while (pid == -1 && errno == EINTR);
        if (pid == -1) {
            perror("waitpid");
            result = 1;
        } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
            result = 1;
    }
    if (have_clock && print_summary() == -1) {
        perror("Журнал");
        result = 1;
    }
    while (initialized > 0) {
        --initialized;
        if (sem_destroy(&boxes[initialized / 2].sem[initialized % 2]) == -1) {
            perror("sem_destroy");
            result = 1;
        }
    }
    if (boxes != MAP_FAILED && munmap(boxes, size) == -1) {
        perror("munmap");
        result = 1;
    }
    if (fd != -1 && close(fd) == -1) {
        perror("close shm");
        result = 1;
    }
    if (created && shm_unlink(name) == -1) {
        perror("shm_unlink");
        result = 1;
    }
    if (log_fd != -1 && close(log_fd) == -1) {
        perror("close log");
        result = 1;
    }
    free(pids);
    free_model();
    return stop_signal ? 128 + stop_signal : result;
}
