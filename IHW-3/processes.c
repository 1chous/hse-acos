#include "simulation.h"

#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef MESSAGE_QUEUES
#include <mqueue.h>
#endif

/* Независимый запуск: elevator-server INPUT SESSION [LOG], затем
 * elevator-client INPUT SESSION ACTOR для каждого номера 1..L+P.
 * Клиенты запускаются после сообщения сервера «Готово» в той же ОС
 * (при работе в Docker — в том же контейнере). */
enum { CAN_WRITE, CAN_READ };
typedef struct {
#ifdef INDEPENDENT
    pid_t registered;
#endif
#ifndef NAMED_SEMAPHORES
    sem_t sem[2];
#endif
    Message message;
} Mailbox;

static Mailbox *boxes = MAP_FAILED;
static int closing;
static pid_t *pids;

#ifdef INDEPENDENT
/* Все участники читают один входной файл. Сервер публикует параметры
 * и общее начало отсчёта; START разрешает читать готовое время старта. */
typedef struct {
    Config config;
    struct timespec started;
    pid_t server;
    Mailbox boxes[];
} Session;

static Session *session = MAP_FAILED;
static sem_t *ready = SEM_FAILED;
static int client, actors;
static time_t closing_deadline;

static int begin_closing(void)
{
    struct timespec now;
    closing = 1;
    if (clock_gettime(CLOCK_MONOTONIC, &now) == -1)
        return -1;
    closing_deadline = now.tv_sec + 5;
    return 0;
}

static int check_peers(void)
{
    if (closing) {
        struct timespec now;
        if (clock_gettime(CLOCK_MONOTONIC, &now) == -1)
            return -1;
        if (now.tv_sec >= closing_deadline) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
    if (client && session->server > 0) {
        if (kill(session->server, 0) == -1)
            return -1;
    } else if (!client && !closing) {
        for (int i = 0; i < actors; ++i) {
            if (pids[i] && kill(pids[i], 0) == -1)
                return -1;
        }
    }
    return 0;
}
#endif

#ifdef NAMED_SEMAPHORES
/* Указатели хранятся в обычной памяти. Дети наследуют их при fork,
 * а независимые участники получают собственные через sem_open. */
static sem_t **semaphores;

static int object_name(char *buffer, size_t size, const char *prefix, size_t index)
{
    int length = snprintf(buffer, size, "%s_%zu", prefix, index);
    if (length < 0 || (size_t)length >= size) {
        errno = EOVERFLOW;
        return -1;
    }
    return 0;
}
#endif

#ifdef MESSAGE_QUEUES
static mqd_t *queues, reply_queue = (mqd_t)-1;
static char reply_name[96];
static int reply_created;

static int reply_queue_name(char *buffer, size_t size, const char *prefix, pid_t pid)
{
    int length = snprintf(buffer, size, "%s_reply_%ld", prefix, (long)pid);
    if (length < 0 || (size_t)length >= size) {
        errno = EOVERFLOW;
        return -1;
    }
    return 0;
}

/* У регистрации своя очередь по PID. Поэтому отказ второму клиенту
 * с тем же номером не попадёт в очередь уже работающего участника. */
static int registration_reply(const char *prefix, pid_t pid, int error)
{
    char name[96];
    if (reply_queue_name(name, sizeof name, prefix, pid) == -1)
        return -1;
    mqd_t queue = mq_open(name, O_WRONLY | O_NONBLOCK);
    if (queue == (mqd_t)-1)
        return -1;
    Message message = { .kind = error ? REJECT : REGISTER,
                       .value = error ? error : (int)getpid() };
    int result = mq_send(queue, (const char *)&message, sizeof message, 0);
    int saved = errno;
    if (mq_close(queue) == -1) {
        result = -1;
        saved = errno;
    }
    if (mq_unlink(name) == -1 && errno != ENOENT) {
        result = -1;
        saved = errno;
    }
    errno = saved;
    return result;
}

static int register_client(const char *prefix, int actor)
{
    if (reply_queue_name(reply_name, sizeof reply_name, prefix, getpid()) == -1)
        return -1;
    struct mq_attr attributes = { .mq_maxmsg = 1, .mq_msgsize = sizeof(Message) };
    reply_queue = mq_open(reply_name, O_CREAT | O_EXCL | O_RDWR, 0600, &attributes);
    if (reply_queue == (mqd_t)-1)
        return -1;
    reply_created = 1;
    Message message = { .kind = REGISTER, .actor = actor,
                        .value = (int)getpid(), .config = config };
    if (send_message(0, message) == -1)
        return -1;
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) == -1)
        return -1;
    deadline.tv_sec += 5;
    ssize_t received;
    do {
        received = mq_timedreceive(reply_queue, (char *)&message, sizeof message, NULL, &deadline);
    } while (received == -1 && errno == EINTR && !stop_signal);
    if (received == -1)
        return -1;
    if (received != (ssize_t)sizeof message || message.kind != REGISTER || message.value <= 0) {
        errno = received == (ssize_t)sizeof message && message.kind == REJECT ?
            message.value : EPROTO;
        return -1;
    }
    session->server = (pid_t)message.value;
    return 0;
}
#endif

static sem_t *box_sem(int receiver, int operation)
{
#ifdef NAMED_SEMAPHORES
    return semaphores[2 * (size_t)receiver + operation];
#else
    return &boxes[receiver].sem[operation];
#endif
}

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
#ifdef INDEPENDENT
        if (check_peers() == -1)
            return -1;
#endif
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
    if (wait_sem(box_sem(receiver, CAN_WRITE)) == -1)
        return -1;
#ifdef MESSAGE_QUEUES
    if (mq_send(queues[receiver], (const char *)&message, sizeof message, 0) == -1)
        return -1;
#else
    boxes[receiver].message = message;
#endif
    return sem_post(box_sem(receiver, CAN_READ));
}

int receive_message(int receiver, Message *message)
{
    if (wait_sem(box_sem(receiver, CAN_READ)) == -1)
        return -1;
#ifdef MESSAGE_QUEUES
    ssize_t received = mq_receive(queues[receiver], (char *)message, sizeof *message, NULL);
    if (received == -1)
        return -1;
    if (received != (ssize_t)sizeof *message) {
        errno = EPROTO;
        return -1;
    }
#else
    *message = boxes[receiver].message;
#endif
#ifdef INDEPENDENT
    /* BYE подтверждает окончание работы роли; waitpid для чужого ребёнка
     * неприменим. На ошибке модели это подтверждение тоже учитываем. */
    if (!client && message->kind == BYE && message->actor >= 1 &&
        message->actor <= actors)
        pids[message->actor - 1] = 0;
#endif
    return sem_post(box_sem(receiver, CAN_WRITE));
}

int main(int argc, char **argv)
{
#ifdef INDEPENDENT
#ifdef CLIENT
    client = 1;
#endif
    if ((client && argc != 4) || (!client && (argc < 3 || argc > 4))) {
        dprintf(STDERR_FILENO, "Использование: %s INPUT SESSION %s\n",
                argv[0], client ? "ACTOR" : "[LOG]");
        return 2;
    }
#else
    if (argc < 2 || argc > 3) {
        dprintf(STDERR_FILENO, "Использование: %s INPUT [LOG]\n", argv[0]);
        return 2;
    }
#endif
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
    int result = 1, fd = -1, created = 0, have_clock = 0;
    size_t initialized = 0;
#ifdef INDEPENDENT
    actors = config.lifts + config.passengers;
    int actor_id = 0, joined = 0, ready_created = 0;
#else
    int actors = config.lifts + config.passengers, spawned = 0;
#endif
    size_t count = (size_t)actors + 1, size = 0;
    char name[64];
#ifdef MESSAGE_QUEUES
    size_t opened_queues = 0;
#endif
#ifdef NAMED_SEMAPHORES
    char sem_name[96];
#endif
#ifdef INDEPENDENT
    if (client) {
        char *end;
        errno = 0;
        long value = strtol(argv[3], &end, 10);
        if (errno || !argv[3][0] || *end || value < 1 || value > actors) {
            errno = EINVAL;
            goto fail;
        }
        actor_id = (int)value;
    }
    if (!argv[2][0] || strchr(argv[2], '/')) {
        errno = EINVAL;
        goto fail;
    }
#ifdef MESSAGE_QUEUES
    int length = snprintf(name, sizeof name, "/acos03_mq_%s", argv[2]);
#else
    int length = snprintf(name, sizeof name, "/acos03_run_%s", argv[2]);
#endif
#else
    int length = snprintf(name, sizeof name, "/acos03_%ld", (long)getpid());
#endif
    if (length < 0 || (size_t)length >= sizeof name) {
        errno = EOVERFLOW;
        goto fail;
    }
#ifdef INDEPENDENT
    if (!client) {
        log_fd = open(argc == 4 ? argv[3] : "elevator-03.log", O_WRONLY | O_CREAT | O_TRUNC, 0600);
#else
    {
        log_fd = open(argc == 3 ? argv[2] : "elevator-03.log", O_WRONLY | O_CREAT | O_TRUNC, 0600);
#endif
        if (log_fd == -1)
            goto fail;
        pids = calloc((size_t)actors, sizeof *pids);
        if (!pids)
            goto fail;
    }
    if (count > SIZE_MAX / sizeof *boxes) {
        errno = EOVERFLOW;
        goto fail;
    }
    size = count * sizeof *boxes;
#ifdef INDEPENDENT
    if (size > SIZE_MAX - sizeof *session) {
        errno = EOVERFLOW;
        goto fail;
    }
    size += sizeof *session;
#endif
    if ((off_t)size < 0 || (uintmax_t)(off_t)size != (uintmax_t)size) {
        errno = EOVERFLOW;
        goto fail;
    }
#ifdef INDEPENDENT
    if (client) {
        if (object_name(sem_name, sizeof sem_name, name, SIZE_MAX) == -1)
            goto fail;
        ready = sem_open(sem_name, 0);
        if (ready == SEM_FAILED)
            goto fail;
    }
#endif
#ifdef MESSAGE_QUEUES
    /* Здесь Session и boxes — обычная частная память процесса.
     * Между процессами данные передаются исключительно через очереди. */
    session = calloc(1, size);
    if (!session) {
        session = MAP_FAILED;
        goto fail;
    }
    boxes = session->boxes;
    queues = calloc(count, sizeof *queues);
    if (!queues)
        goto fail;
    struct mq_attr attributes = { .mq_maxmsg = 1, .mq_msgsize = sizeof(Message) };
    for (; opened_queues < count; ++opened_queues) {
        if (object_name(sem_name, sizeof sem_name, name, opened_queues) == -1)
            goto fail;
        int flags = O_RDWR | O_NONBLOCK;
        if (!client)
            flags |= O_CREAT | O_EXCL;
        queues[opened_queues] = mq_open(sem_name, flags, 0600, &attributes);
        if (queues[opened_queues] == (mqd_t)-1)
            goto fail;
        if (!client)
            created = 1;
    }
#else
#ifdef INDEPENDENT
    fd = shm_open(name, client ? O_RDWR : O_CREAT | O_EXCL | O_RDWR, 0600);
#else
    fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
#endif
    if (fd == -1)
        goto fail;
#ifdef INDEPENDENT
    created = !client;
    if (client) {
        struct stat info;
        if (fstat(fd, &info) == -1)
            goto fail;
        if (info.st_size != (off_t)size) {
            errno = EINVAL;
            goto fail;
        }
    }
#else
    created = 1;
#endif
    if (created && ftruncate(fd, (off_t)size) == -1)
        goto fail;
#ifdef INDEPENDENT
    session = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (session == MAP_FAILED)
        goto fail;
    boxes = session->boxes;
#else
    boxes = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (boxes == MAP_FAILED)
        goto fail;
#endif
    int close_result = close(fd);
    fd = -1;
    if (close_result == -1)
        goto fail;
#endif
#ifdef NAMED_SEMAPHORES
    semaphores = calloc(2 * count, sizeof *semaphores);
    if (!semaphores)
        goto fail;
#endif
    for (; initialized < 2 * count; ++initialized) {
#ifdef NAMED_SEMAPHORES
        if (object_name(sem_name, sizeof sem_name, name, initialized) == -1)
            goto fail;
#ifdef INDEPENDENT
        int flags = client ? 0 : O_CREAT | O_EXCL;
#else
        int flags = O_CREAT | O_EXCL;
#endif
        semaphores[initialized] = sem_open(sem_name, flags, 0600,
                                          (unsigned)(initialized % 2 == CAN_WRITE));
        if (semaphores[initialized] == SEM_FAILED)
            goto fail;
#else
        sem_t *sem = &boxes[initialized / 2].sem[initialized % 2];
        if (sem_init(sem, 1, initialized % 2 == CAN_WRITE) == -1)
            goto fail;
#endif
    }
#ifdef INDEPENDENT
    if (client) {
#ifdef MESSAGE_QUEUES
        if (register_client(name, actor_id) == -1)
            goto fail;
        joined = 1;
#else
        if (session->server <= 0) {
            errno = EPROTO;
            goto fail;
        }
        if (wait_sem(ready) == -1)
            goto fail;
        int duplicate = boxes[actor_id].registered != 0;
        int mismatch = memcmp(&config, &session->config, sizeof config) != 0;
        if (!duplicate && !mismatch) {
            boxes[actor_id].registered = getpid();
            joined = 1;
        }
        if (sem_post(ready) == -1)
            goto fail;
        if (duplicate || mismatch) {
            errno = duplicate ? EEXIST : EINVAL;
            goto fail;
        }
        if (send_message(0, (Message){ .kind = REGISTER, .actor = actor_id,
                                     .value = (int)getpid() }) == -1)
            goto fail;
#endif
        Message message;
        if (receive_message(actor_id, &message) == -1 || message.kind != START) {
            if (!stop_signal)
                errno = EPROTO;
            goto fail;
        }
#ifdef MESSAGE_QUEUES
        started = message.started;
#else
        started = session->started;
#endif
        if (actor_main(actor_id) == -1)
            goto fail;
        result = 0;
        goto cleanup;
    }
    session->config = config;
    session->server = getpid();
    /* Позволяет вывести итог и при остановке до подключения всех клиентов. */
    if (clock_gettime(CLOCK_MONOTONIC, &started) == -1)
        goto fail;
    have_clock = 1;
    if (object_name(sem_name, sizeof sem_name, name, SIZE_MAX) == -1)
        goto fail;
    ready = sem_open(sem_name, O_CREAT | O_EXCL, 0600, 0U);
    if (ready == SEM_FAILED)
        goto fail;
    ready_created = 1;
    if (sem_post(ready) == -1)
        goto fail;
    if (dprintf(STDOUT_FILENO, "Готово: сеанс %s; запустите участников 1..%d\n",
                argv[2], actors) < 0)
        goto fail;
    int connected = 0;
    while (connected < actors) {
        Message message;
        if (receive_message(0, &message) == -1)
            goto fail;
        if (message.kind != REGISTER || message.value <= 0) {
            errno = EPROTO;
            goto fail;
        }
        int error = 0;
        if (message.actor < 1 || message.actor > actors)
            error = EINVAL;
        else if (pids[message.actor - 1])
            error = EEXIST;
#ifdef MESSAGE_QUEUES
        else if (memcmp(&config, &message.config, sizeof config) != 0)
            error = EINVAL;
        if (registration_reply(name, (pid_t)message.value, error) == -1)
            goto fail;
        if (error)
            continue;
#else
        if (error) {
            errno = error;
            goto fail;
        }
#endif
        pids[message.actor - 1] = (pid_t)message.value;
        ++connected;
    }
#endif
    if (clock_gettime(CLOCK_MONOTONIC, &started) == -1)
        goto fail;
    have_clock = 1;
#ifdef INDEPENDENT
    session->started = started;
    for (int actor = 1; actor <= actors; ++actor) {
        if (send_message(actor, (Message){ .kind = START, .started = started }) == -1)
            goto fail;
    }
#else
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
#endif
    if (dispatcher_main() == -1)
        goto fail;
    /* После STOP нормальное завершение детей больше не является ошибкой. */
#ifdef INDEPENDENT
    if (begin_closing() == -1)
        goto fail;
#else
    closing = 1;
#endif
    for (int actor = 1; actor <= actors; ++actor) {
        if (send_message(actor, (Message){ .kind = STOP }) == -1)
            goto fail;
    }
    result = 0;
    goto cleanup;
fail:
    if (!stop_signal)
        perror("Моделирование");
cleanup:;
#ifdef INDEPENDENT
    int saved_signal = stop_signal;
    stop_signal = 0;
    int can_wait = begin_closing() == 0;
    if (!can_wait)
        result = 1;
    if (client) {
        if (joined && can_wait && send_message(0, (Message){ .kind = BYE,
                .actor = actor_id, .value = result }) == -1)
            result = 1;
    } else if (pids) {
        if (result != 0 || saved_signal) {
            for (int i = 0; i < actors; ++i) {
                if (pids[i] && kill(pids[i], SIGTERM) == -1 && errno != ESRCH)
                    perror("kill");
            }
        }
        while (can_wait) {
            int remaining = 0;
            for (int i = 0; i < actors; ++i)
                remaining += pids[i] != 0;
            if (!remaining)
                break;
            Message message;
            if (receive_message(0, &message) == -1) {
                perror("Завершение участников");
                result = 1;
                break;
            }
            if (message.kind == BYE && message.value != 0)
                result = 1;
        }
    }
    if (saved_signal)
        stop_signal = saved_signal;
#else
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
#endif
    if (have_clock && print_summary() == -1) {
        perror("Журнал");
        result = 1;
    }
    while (initialized > 0) {
        --initialized;
#ifdef NAMED_SEMAPHORES
        if (sem_close(semaphores[initialized]) == -1) {
            perror("sem_close");
            result = 1;
        }
        if (created && (object_name(sem_name, sizeof sem_name, name, initialized) == -1 ||
            sem_unlink(sem_name) == -1)) {
            perror("sem_unlink");
            result = 1;
        }
#else
        if (sem_destroy(&boxes[initialized / 2].sem[initialized % 2]) == -1) {
            perror("sem_destroy");
            result = 1;
        }
#endif
    }
#ifdef INDEPENDENT
    if (ready != SEM_FAILED && sem_close(ready) == -1) {
        perror("sem_close ready");
        result = 1;
    }
    if (ready_created && (object_name(sem_name, sizeof sem_name, name, SIZE_MAX) == -1 ||
        sem_unlink(sem_name) == -1)) {
        perror("sem_unlink ready");
        result = 1;
    }
#endif
#ifdef MESSAGE_QUEUES
    if (reply_queue != (mqd_t)-1 && mq_close(reply_queue) == -1) {
        perror("mq_close reply");
        result = 1;
    }
    if (reply_created && mq_unlink(reply_name) == -1 && errno != ENOENT) {
        perror("mq_unlink reply");
        result = 1;
    }
    while (opened_queues > 0) {
        --opened_queues;
        if (mq_close(queues[opened_queues]) == -1) {
            perror("mq_close");
            result = 1;
        }
        if (created && (object_name(sem_name, sizeof sem_name, name, opened_queues) == -1 ||
            mq_unlink(sem_name) == -1)) {
            perror("mq_unlink");
            result = 1;
        }
    }
    free(queues);
    if (session != MAP_FAILED)
        free(session);
#else
#ifdef INDEPENDENT
    if (session != MAP_FAILED && munmap(session, size) == -1) {
#else
    if (boxes != MAP_FAILED && munmap(boxes, size) == -1) {
#endif
        perror("munmap");
        result = 1;
    }
#endif
    if (fd != -1 && close(fd) == -1) {
        perror("close shm");
        result = 1;
    }
#ifndef MESSAGE_QUEUES
    if (created && shm_unlink(name) == -1) {
        perror("shm_unlink");
        result = 1;
    }
#endif
    if (log_fd != -1 && close(log_fd) == -1) {
        perror("close log");
        result = 1;
    }
    free(pids);
#ifdef NAMED_SEMAPHORES
    free(semaphores);
#endif
    free_model();
    return stop_signal ? 128 + stop_signal : result;
}
