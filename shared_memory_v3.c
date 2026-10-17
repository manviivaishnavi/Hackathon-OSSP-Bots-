#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <errno.h>
#include <signal.h>

#define SHM_NAME "/shared_memory_v3_demo"
#define BUFFER_SIZE 5
#define MESSAGE_SIZE 128
#define MAX_PRODUCERS 8
#define MAX_CONSUMERS 8
#define MAX_MESSAGES 10000

typedef struct {
    int id;
    int producer_id;
    int stop;
    char text[MESSAGE_SIZE];
} Message;

typedef struct {
    sem_t empty;
    sem_t full;
    sem_t mutex;
    int in;
    int out;
    int next_id;
    int consumed;
    Message buffer[BUFFER_SIZE];
} SharedData;

static int wait_sem(sem_t *sem) {
    while (sem_wait(sem) == -1) {
        if (errno == EINTR)
            continue;
        perror("sem_wait");
        return -1;
    }
    return 0;
}

static int post_sem(sem_t *sem) {
    while (sem_post(sem) == -1) {
        if (errno == EINTR)
            continue;
        perror("sem_post");
        return -1;
    }
    return 0;
}

static int read_number(const char *s, int min, int max) {
    char *end;
    errno = 0;
    long value = strtol(s, &end, 10);

    if (errno || *s == '\0' || *end != '\0' ||
        value < min || value > max)
        return -1;

    return (int)value;
}

int main(int argc, char **argv) {
    int producers = 2;
    int consumers = 2;
    int messages = 20;

    if (argc == 4) {
        producers = read_number(argv[1], 1, MAX_PRODUCERS);
        consumers = read_number(argv[2], 1, MAX_CONSUMERS);
        messages = read_number(argv[3], 1, MAX_MESSAGES);

        if (producers < 1 || consumers < 1 || messages < 1) {
            fprintf(stderr,
                    "Usage: %s [producers consumers messages]\n",
                    argv[0]);
            return 1;
        }
    } else if (argc != 1) {
        fprintf(stderr,
                "Usage: %s [producers consumers messages]\n",
                argv[0]);
        return 1;
    }

    setbuf(stdout, NULL);

    int fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);

    if (fd == -1) {
        perror("shm_open");
        fprintf(stderr,
                "If a previous run crashed, remove stale shared memory "
                "before retrying.\n");
        return 1;
    }

    if (ftruncate(fd, sizeof(SharedData)) == -1) {
        perror("ftruncate");
        close(fd);
        shm_unlink(SHM_NAME);
        return 1;
    }

    SharedData *data = mmap(NULL, sizeof(*data),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, 0);

    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        shm_unlink(SHM_NAME);
        return 1;
    }

    memset(data, 0, sizeof(*data));

    int initialized = 0;

    if (sem_init(&data->empty, 1, BUFFER_SIZE) == -1)
        goto setup_failure;
    initialized = 1;

    if (sem_init(&data->full, 1, 0) == -1)
        goto setup_failure;
    initialized = 2;

    if (sem_init(&data->mutex, 1, 1) == -1)
        goto setup_failure;
    initialized = 3;

    pid_t children[MAX_PRODUCERS + MAX_CONSUMERS];
    int child_count = 0;

    printf("Shared Memory Data Exchange System\n");
    printf("Producers: %d | Consumers: %d | Messages: %d\n\n",
           producers, consumers, messages);

    /* Create consumer processes */
    for (int i = 0; i < consumers; i++) {
        pid_t pid = fork();

        if (pid == -1) {
            perror("fork consumer");
            goto runtime_failure;
        }

        if (pid == 0) {
            while (1) {
                if (wait_sem(&data->full) == -1)
                    _exit(1);

                if (wait_sem(&data->mutex) == -1)
                    _exit(1);

                Message msg = data->buffer[data->out];
                data->out = (data->out + 1) % BUFFER_SIZE;

                if (!msg.stop)
                    data->consumed++;

                if (post_sem(&data->mutex) == -1)
                    _exit(1);

                if (post_sem(&data->empty) == -1)
                    _exit(1);

                if (msg.stop) {
                    printf("[Consumer %d] Stopping.\n", i + 1);
                    _exit(0);
                }

                printf("[Consumer %d] Read ID %d from Producer %d: %s\n",
                       i + 1, msg.id, msg.producer_id, msg.text);
            }
        }

        children[child_count++] = pid;
    }

    /* Create producer processes */
    for (int i = 0; i < producers; i++) {
        pid_t pid = fork();

        if (pid == -1) {
            perror("fork producer");
            goto runtime_failure;
        }

        if (pid == 0) {
            while (1) {
                if (wait_sem(&data->empty) == -1)
                    _exit(1);

                if (wait_sem(&data->mutex) == -1) {
                    post_sem(&data->empty);
                    _exit(1);
                }

                if (data->next_id >= messages) {
                    post_sem(&data->mutex);
                    post_sem(&data->empty);
                    break;
                }

                int id = ++data->next_id;
                int slot = data->in;

                data->buffer[slot].id = id;
                data->buffer[slot].producer_id = i + 1;
                data->buffer[slot].stop = 0;

                snprintf(data->buffer[slot].text, MESSAGE_SIZE,
                         "Message %d generated by Producer %d",
                         id, i + 1);

                data->in = (data->in + 1) % BUFFER_SIZE;

                printf("[Producer %d] Wrote ID %d into slot %d\n",
                       i + 1, id, slot);

                if (post_sem(&data->mutex) == -1)
                    _exit(1);

                if (post_sem(&data->full) == -1)
                    _exit(1);
            }

            _exit(0);
        }

        children[child_count++] = pid;
    }

    /* Wait for every producer */
    for (int i = consumers; i < child_count; i++) {
        int status;

        if (waitpid(children[i], &status, 0) == -1 ||
            !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "A producer process failed.\n");
            goto runtime_failure;
        }
    }

    /* Send one stop message to each consumer */
    for (int i = 0; i < consumers; i++) {
        if (wait_sem(&data->empty) == -1)
            goto runtime_failure;

        if (wait_sem(&data->mutex) == -1) {
            post_sem(&data->empty);
            goto runtime_failure;
        }

        int slot = data->in;

        data->buffer[slot].id = 0;
        data->buffer[slot].producer_id = 0;
        data->buffer[slot].stop = 1;

        snprintf(data->buffer[slot].text, MESSAGE_SIZE, "STOP");

        data->in = (data->in + 1) % BUFFER_SIZE;

        if (post_sem(&data->mutex) == -1)
            goto runtime_failure;

        if (post_sem(&data->full) == -1)
            goto runtime_failure;
    }

    /* Wait for every consumer */
    for (int i = 0; i < consumers; i++) {
        int status;

        if (waitpid(children[i], &status, 0) == -1 ||
            !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "A consumer process failed.\n");
            goto runtime_failure;
        }
    }

    if (data->next_id != messages ||
        data->consumed != messages) {
        fprintf(stderr,
                "ERROR: Expected %d messages, produced %d, consumed %d.\n",
                messages, data->next_id, data->consumed);
        goto runtime_failure;
    }

    sem_destroy(&data->empty);
    sem_destroy(&data->full);
    sem_destroy(&data->mutex);

    munmap(data, sizeof(*data));
    close(fd);
    shm_unlink(SHM_NAME);

    printf("\nAll %d messages were produced and consumed.\n", messages);
    printf("Verified: produced = %d, consumed = %d.\n",
           messages, messages);
    printf("Shared memory cleaned up successfully.\n");
    printf("System finished successfully.\n");

    return 0;

runtime_failure:
    for (int i = 0; i < child_count; i++)
        kill(children[i], SIGTERM);

    for (int i = 0; i < child_count; i++)
        waitpid(children[i], NULL, 0);

    sem_destroy(&data->empty);
    sem_destroy(&data->full);
    sem_destroy(&data->mutex);

    munmap(data, sizeof(*data));
    close(fd);
    shm_unlink(SHM_NAME);

    return 1;

setup_failure:
    perror("sem_init");

    if (initialized >= 1)
        sem_destroy(&data->empty);

    if (initialized >= 2)
        sem_destroy(&data->full);

    munmap(data, sizeof(*data));
    close(fd);
    shm_unlink(SHM_NAME);

    return 1;
}

