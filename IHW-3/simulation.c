#include "simulation.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

enum { FUTURE, WAITING, RESERVED, RIDING, DELIVERED };
enum { IDLE, MOVING, OPENING, TRANSFER, CLOSING };

typedef struct {
    int from, to, state, lift, order;
    long long arrival, appeared, boarded;
} Passenger;

typedef struct {
    int floor, target, direction, load, phase, doors, pickup;
} Lift;

Config config;
struct timespec started;
volatile sig_atomic_t stop_signal, child_changed;
int log_fd = -1;
static Passenger *people;
static Lift *lifts;

static int invalid(void)
{
    errno = EINVAL;
    return -1;
}

/* Все поля входного файла — неотрицательные целые числа. */
static int number(int fd)
{
    unsigned char c;
    ssize_t n;
    do {
        n = read(fd, &c, 1);
        if (n != 1)
            return n == 0 ? invalid() : -1;
    } while (isspace(c));
    if (!isdigit(c))
        return invalid();
    int value = 0;
    do {
        int digit = c - '0';
        if (value > (INT_MAX - digit) / 10)
            return invalid();
        value = value * 10 + digit;
        n = read(fd, &c, 1);
        if (n == -1)
            return -1;
    } while (n == 1 && isdigit(c));
    if (n == 1 && !isspace(c))
        return invalid();
    return value;
}

/* Формат (переносы строк произвольные):
 * floors lifts capacity passengers seed strategy
 * arrival_min arrival_max travel_ms door_ms board_ms leave_ms
 * начальный этаж каждого лифта; затем пары from to для пассажиров.
 * Все времена — миллисекунды; arrival_min/max — интервалы появлений.
 * strategy=0: старейший вызов, ближайший свободный лифт;
 * strategy=1: ближайшая пара «свободный лифт — ожидающий пассажир».
 * После назначения забираем группу с одного этажа и направления.
 * Попутные новые вызовы в текущий рейс не добавляем.
 */
int load_input(const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd == -1)
        return -1;
    int *fields[] = { &config.floors, &config.lifts, &config.capacity,
        &config.passengers, &config.seed, &config.strategy,
        &config.arrival_min, &config.arrival_max, &config.travel_ms,
        &config.door_ms, &config.board_ms, &config.leave_ms };
    for (size_t i = 0; i < sizeof fields / sizeof fields[0]; ++i) {
        *fields[i] = number(fd);
        if (*fields[i] == -1)
            goto fail;
    }
    if (config.floors < 2 || config.lifts < 1 || config.capacity < 1 ||
        config.lifts > INT_MAX - 1 - config.passengers ||
        config.arrival_min > config.arrival_max || config.strategy > 1) {
        invalid();
        goto fail;
    }
    people = calloc(config.passengers ? (size_t)config.passengers : 1, sizeof *people);
    lifts = calloc((size_t)config.lifts, sizeof *lifts);
    if (!people || !lifts)
        goto fail;
    for (int j = 0; j < config.lifts; ++j) {
        lifts[j].floor = number(fd);
        if (lifts[j].floor < 1 || lifts[j].floor > config.floors) {
            invalid();
            goto fail;
        }
    }
    srand((unsigned)config.seed);
    long long arrival = 0;
    unsigned span = (unsigned)config.arrival_max - config.arrival_min + 1;
    for (int i = 0; i < config.passengers; ++i) {
        Passenger *p = &people[i];
        p->from = number(fd);
        p->to = number(fd);
        if (p->from < 1 || p->from > config.floors || p->to < 1 ||
            p->to > config.floors || p->from == p->to) {
            invalid();
            goto fail;
        }
        arrival += config.arrival_min + (unsigned)rand() % span;
        p->arrival = arrival;
        p->lift = -1;
    }
    unsigned char c;
    ssize_t n;
    while ((n = read(fd, &c, 1)) == 1) {
        if (!isspace(c)) {
            invalid();
            goto fail;
        }
    }
    if (n == -1)
        goto fail;
    return close(fd);
fail:;
    int saved = errno;
    close(fd);
    free_model();
    errno = saved;
    return -1;
}

void free_model(void)
{
    free(people);
    free(lifts);
    people = NULL;
    lifts = NULL;
}

static long long now_ms(void)
{
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) == -1)
        return -1;
    long long seconds = (long long)now.tv_sec - started.tv_sec;
    long ns = now.tv_nsec - started.tv_nsec;
    if (ns < 0) {
        --seconds;
        ns += 1000000000L;
    }
    return seconds * 1000 + ns / 1000000;
}

/* Журнал пишет только диспетчер, поэтому строки не перемешиваются. */
static int report(const char *format, ...)
{
    char text[512];
    va_list args;
    va_start(args, format);
    int n = vsnprintf(text, sizeof text, format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof text) {
        errno = EOVERFLOW;
        return -1;
    }
    long long time = now_ms();
    if (time == -1 || dprintf(STDOUT_FILENO, "%8lld ms | %s\n", time, text) < 0)
        return -1;
    return dprintf(log_fd, "%8lld ms | %s\n", time, text) < 0 ? -1 : 0;
}

static int delay_ms(long long ms)
{
    /* Короткие отрезки исключают долгое ожидание, если сигнал пришёл
     * между проверкой stop_signal и входом в nanosleep. */
    while (ms > 0 && !stop_signal) {
        long step = ms > 100 ? 100 : (long)ms;
        struct timespec delay = { .tv_sec = 0, .tv_nsec = step * 1000000L };
        while (nanosleep(&delay, &delay) == -1) {
            if (errno != EINTR || stop_signal)
                return -1;
        }
        ms -= step;
    }
    if (stop_signal) {
        errno = EINTR;
        return -1;
    }
    return 0;
}

/* Номера участников: 0 — диспетчер, 1..L — лифты, далее пассажиры. */
int actor_main(int actor)
{
    int passenger = actor > config.lifts;
    if (passenger) {
        long long now = now_ms();
        if (now == -1 || delay_ms(people[actor - config.lifts - 1].arrival - now) == -1)
            return -1;
        if (send_message(0, (Message){ APPEAR, actor, 0 }) == -1)
            return -1;
    }
    for (;;) {
        Message message;
        if (receive_message(actor, &message) == -1)
            return -1;
        if (message.kind == STOP)
            return 0;
        int ms;
        if (passenger && message.kind == BOARD)
            ms = config.board_ms;
        else if (passenger && message.kind == LEAVE)
            ms = config.leave_ms;
        else if (!passenger && message.kind == MOVE)
            ms = config.travel_ms;
        else if (!passenger && (message.kind == OPEN || message.kind == CLOSE))
            ms = config.door_ms;
        else
            return invalid();
        if (delay_ms(ms) == -1)
            return -1;
        message.actor = actor;
        if (send_message(0, message) == -1)
            return -1;
    }
}

static int lift_command(int j, int kind)
{
    Lift *l = &lifts[j];
    int value = l->floor;
    if (kind == MOVE) {
        if (l->doors || l->floor == l->target)
            return invalid();
        value += l->target > l->floor ? 1 : -1;
        l->phase = MOVING;
        if (report("Лифт %d: начало движения %d -> %d", j + 1, l->floor, value) == -1)
            return -1;
    } else {
        if ((kind == OPEN && l->doors) || (kind == CLOSE && !l->doors))
            return invalid();
        l->phase = kind == OPEN ? OPENING : CLOSING;
        if (report("Лифт %d: %s дверей, этаж %d", j + 1,
                   kind == OPEN ? "открытие" : "закрытие", l->floor) == -1)
            return -1;
    }
    return send_message(j + 1, (Message){ kind, 0, value });
}

static int approach(int j)
{
    return lift_command(j, lifts[j].floor == lifts[j].target ? OPEN : MOVE);
}

/* Посадка и высадка идут по одному человеку при открытых дверях. */
static int transfer(int j)
{
    Lift *l = &lifts[j];
    for (int i = 0; i < config.passengers; ++i) {
        Passenger *p = &people[i];
        if (p->lift != j)
            continue;
        if ((l->pickup && p->state == RESERVED) ||
            (!l->pickup && p->state == RIDING && p->to == l->floor)) {
            l->phase = TRANSFER;
            int kind = l->pickup ? BOARD : LEAVE;
            if (report("Пассажир %d: начало %s, лифт %d, этаж %d", i + 1,
                       l->pickup ? "посадки" : "высадки", j + 1, l->floor) == -1)
                return -1;
            return send_message(config.lifts + 1 + i, (Message){ kind, 0, j });
        }
    }
    return lift_command(j, CLOSE);
}

static int next_stop(int j)
{
    Lift *l = &lifts[j];
    if (l->load == 0) {
        l->phase = IDLE;
        return 0;
    }
    int target = 0;
    for (int i = 0; i < config.passengers; ++i) {
        Passenger *p = &people[i];
        if (p->state == RIDING && p->lift == j &&
            (!target || abs(p->to - l->floor) < abs(target - l->floor)))
            target = p->to;
    }
    if (!target || (target - l->floor) * l->direction <= 0)
        return invalid();
    l->target = target;
    return approach(j);
}

static int reserve(int i, int j)
{
    people[i].state = RESERVED;
    people[i].lift = j;
    return report("Вызов пассажира %d назначен лифту %d", i + 1, j + 1);
}

static int assign_calls(void)
{
    for (;;) {
        int selected = -1, chosen = -1, distance = INT_MAX;
        for (int i = 0; i < config.passengers; ++i) {
            if (people[i].state != WAITING)
                continue;
            for (int j = 0; j < config.lifts; ++j) {
                if (lifts[j].phase != IDLE)
                    continue;
                int d = abs(lifts[j].floor - people[i].from);
                int better = selected == -1;
                if (!better && config.strategy == 0)
                    better = people[i].order < people[selected].order ||
                        (people[i].order == people[selected].order && d < distance);
                if (!better && config.strategy == 1)
                    better = d < distance ||
                        (d == distance && people[i].order < people[selected].order);
                if (better) {
                    selected = i;
                    chosen = j;
                    distance = d;
                }
            }
        }
        if (selected == -1)
            return 0;
        Lift *l = &lifts[chosen];
        Passenger *first = &people[selected];
        l->target = first->from;
        l->direction = first->to > first->from ? 1 : -1;
        l->pickup = 1;
        if (reserve(selected, chosen) == -1)
            return -1;
        int seats = config.capacity - 1;
        for (int i = 0; i < config.passengers; ++i) {
            Passenger *p = &people[i];
            if (p->state != WAITING || p->from != first->from ||
                (p->to > p->from ? 1 : -1) != l->direction)
                continue;
            if (seats > 0) {
                if (reserve(i, chosen) == -1)
                    return -1;
                --seats;
            } else if (report("Пассажир %d продолжает ждать: места лифта %d распределены",
                              i + 1, chosen + 1) == -1)
                return -1;
        }
        if (approach(chosen) == -1)
            return -1;
    }
}

static int passenger_event(Message message, int *arrived, int *delivered)
{
    int i = message.actor - config.lifts - 1;
    Passenger *p = &people[i];
    long long now = now_ms();
    if (now == -1)
        return -1;
    if (message.kind == APPEAR && p->state == FUTURE) {
        p->state = WAITING;
        p->order = ++*arrived;
        p->appeared = now;
        return report("Пассажир %d появился; вызов зарегистрирован: этаж %d -> %d",
                      i + 1, p->from, p->to);
    }
    if (p->lift < 0 || message.value != p->lift)
        return invalid();
    Lift *l = &lifts[p->lift];
    if (!l->doors || l->phase != TRANSFER)
        return invalid();
    if (message.kind == BOARD && l->pickup && p->state == RESERVED &&
        l->floor == p->from && l->load < config.capacity) {
        p->state = RIDING;
        p->boarded = now;
        ++l->load;
        if (report("Пассажир %d: посадка завершена, лифт %d, занято %d/%d",
                   i + 1, p->lift + 1, l->load, config.capacity) == -1)
            return -1;
    } else if (message.kind == LEAVE && !l->pickup && p->state == RIDING &&
               l->floor == p->to && l->load > 0) {
        p->state = DELIVERED;
        --l->load;
        ++*delivered;
        if (report("Пассажир %d: высадка завершена, доставлен на этаж %d, лифт %d",
                   i + 1, p->to, p->lift + 1) == -1)
            return -1;
    } else
        return invalid();
    return transfer(p->lift);
}

static int lift_event(Message message)
{
    int j = message.actor - 1;
    Lift *l = &lifts[j];
    if (message.kind == MOVE && l->phase == MOVING && !l->doors &&
        message.value == l->floor + (l->target > l->floor ? 1 : -1)) {
        l->floor = message.value;
        if (report("Лифт %d прибыл на этаж %d", j + 1, l->floor) == -1)
            return -1;
        return approach(j);
    }
    if (message.kind == OPEN && l->phase == OPENING && message.value == l->floor) {
        l->doors = 1;
        if (report("Лифт %d: двери открыты, этаж %d", j + 1, l->floor) == -1)
            return -1;
        return transfer(j);
    }
    if (message.kind == CLOSE && l->phase == CLOSING && message.value == l->floor) {
        l->doors = 0;
        l->pickup = 0;
        if (report("Лифт %d: двери закрыты, этаж %d", j + 1, l->floor) == -1)
            return -1;
        return next_stop(j);
    }
    return invalid();
}

int dispatcher_main(void)
{
    int arrived = 0, delivered = 0;
    if (report("Старт: этажей %d, лифтов %d, мест %d, пассажиров %d, seed=%d, стратегия=%d",
               config.floors, config.lifts, config.capacity, config.passengers,
               config.seed, config.strategy) == -1)
        return -1;
    for (;;) {
        if (assign_calls() == -1)
            return -1;
        int active = 0;
        for (int j = 0; j < config.lifts; ++j)
            active += lifts[j].phase != IDLE;
        if (delivered == config.passengers && active == 0)
            return 0;
        Message message;
        if (receive_message(0, &message) == -1)
            return -1;
        if (message.actor < 1 || message.actor > config.lifts + config.passengers)
            return invalid();
        int result = message.actor <= config.lifts ? lift_event(message) :
            passenger_event(message, &arrived, &delivered);
        if (result == -1)
            return -1;
    }
}

int print_summary(void)
{
    static const char *states[] = { "ещё не появился", "ожидает назначения",
        "ожидает посадки", "в лифте", "доставлен" };
    int delivered = 0, boarded = 0;
    long double waiting = 0;
    for (int i = 0; i < config.passengers; ++i) {
        Passenger *p = &people[i];
        delivered += p->state == DELIVERED;
        if (p->state >= RIDING) {
            ++boarded;
            waiting += p->boarded - p->appeared;
        }
    }
    if (report("Итог: доставлено %d/%d; среднее ожидание посадки %.2Lf ms (%d пассажиров)",
               delivered, config.passengers, boarded ? waiting / boarded : 0.0L,
               boarded) == -1)
        return -1;
    for (int i = 0; i < config.passengers; ++i) {
        Passenger *p = &people[i];
        if (p->state != DELIVERED && report("Пассажир %d, %d -> %d: %s; назначенный лифт %d"
                " (последнее подтверждённое состояние)", i + 1, p->from, p->to,
                states[p->state], p->lift + 1) == -1)
            return -1;
    }
    return 0;
}
