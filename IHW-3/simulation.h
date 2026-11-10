#ifndef SIMULATION_H
#define SIMULATION_H

#include <signal.h>
#include <time.h>

enum { APPEAR, MOVE, OPEN, CLOSE, BOARD, LEAVE, STOP, REGISTER, START, BYE, REJECT };

typedef struct {
    int floors, lifts, capacity, passengers, seed, strategy;
    int arrival_min, arrival_max, travel_ms, door_ms, board_ms, leave_ms;
} Config;

typedef struct {
    int kind, actor, value;
    Config config;              /* Параметры участника при регистрации. */
    struct timespec started;    /* Общее время старта в сообщении START. */
} Message;

extern Config config;
extern struct timespec started;
extern volatile sig_atomic_t stop_signal, child_changed;
extern int log_fd;

/* Общая модель; способ передачи сообщений реализован отдельно. */
int load_input(const char *path);
void free_model(void);
int actor_main(int actor);
int dispatcher_main(void);
int print_summary(void);
int send_message(int receiver, Message message);
int receive_message(int receiver, Message *message);

#endif
