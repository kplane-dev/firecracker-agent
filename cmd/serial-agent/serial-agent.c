/*
 * serial-agent: minimal exec agent for Firecracker serial console.
 *
 * Reads command lines from stdin (ttyS0), executes via /bin/sh -c,
 * writes JSON responses to stdout. No runtime — resumes from read()
 * after snapshot restore with zero overhead.
 *
 * Protocol:
 *   Input:  command string (one line, e.g., "node -v\n")
 *   Output: {"stdout":"v22.22.2\n","stderr":"","exit_code":0}\n
 *
 * Compile: gcc -static -O2 -o serial-agent serial-agent.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#define MAX_CMD    65536
#define MAX_OUTPUT (256 * 1024)

static void json_escape(const char *src, int srclen, char *dst, int dstmax) {
    int j = 0;
    for (int i = 0; i < srclen && j < dstmax - 10; i++) {
        switch (src[i]) {
            case '"':  dst[j++] = '\\'; dst[j++] = '"'; break;
            case '\\': dst[j++] = '\\'; dst[j++] = '\\'; break;
            case '\n': dst[j++] = '\\'; dst[j++] = 'n'; break;
            case '\t': dst[j++] = '\\'; dst[j++] = 't'; break;
            case '\r': dst[j++] = '\\'; dst[j++] = 'r'; break;
            default:
                if ((unsigned char)src[i] >= 0x20)
                    dst[j++] = src[i];
                break;
        }
    }
    dst[j] = '\0';
}

int main(void) {
    char line[MAX_CMD];
    char output[MAX_OUTPUT];
    char escaped[MAX_OUTPUT * 2];

    /* Unbuffered I/O for instant response */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    fprintf(stderr, "serial-agent: ready\n");

    while (fgets(line, sizeof(line), stdin)) {
        /* Strip trailing newline/whitespace */
        int len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r' || line[len-1] == ' '))
            line[--len] = '\0';

        if (len == 0) continue;

        /* Create pipe for capturing child output */
        int pipefd[2];
        if (pipe(pipefd) < 0) {
            printf("{\"stdout\":\"\",\"stderr\":\"pipe failed\",\"exit_code\":1}\n");
            continue;
        }

        pid_t pid = fork();
        if (pid < 0) {
            close(pipefd[0]);
            close(pipefd[1]);
            printf("{\"stdout\":\"\",\"stderr\":\"fork failed\",\"exit_code\":1}\n");
            continue;
        }

        if (pid == 0) {
            /* Child: redirect stdout+stderr to pipe, exec command */
            close(pipefd[0]);
            dup2(pipefd[1], STDOUT_FILENO);
            dup2(pipefd[1], STDERR_FILENO);
            close(pipefd[1]);

            /* Set PATH for command resolution */
            setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", 1);
            setenv("HOME", "/root", 1);

            char *argv[] = {"/bin/sh", "-c", line, NULL};
            execvp("/bin/sh", argv);
            _exit(127);
        }

        /* Parent: read child output */
        close(pipefd[1]);

        int total = 0;
        int n;
        while ((n = read(pipefd[0], output + total, MAX_OUTPUT - total - 1)) > 0) {
            total += n;
        }
        output[total] = '\0';
        close(pipefd[0]);

        /* Wait for child */
        int status;
        waitpid(pid, &status, 0);
        int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;

        /* Escape output for JSON */
        json_escape(output, total, escaped, sizeof(escaped));

        /* Write JSON response */
        printf("{\"stdout\":\"%s\",\"stderr\":\"\",\"exit_code\":%d}\n", escaped, exit_code);
    }

    return 0;
}
