/* Keep the libjuice config ABI in C; Rust only sees opaque agents/callbacks. */
#include <juice/juice.h>
#include <string.h>

typedef void (*state_cb)(juice_agent_t *, juice_state_t, void *);
typedef void (*done_cb)(juice_agent_t *, void *);
typedef void (*recv_cb)(juice_agent_t *, const char *, size_t, void *);

juice_agent_t *phantom_ice_create(const char *bind, const char *stun, unsigned short port,
    state_cb state, done_cb done, recv_cb recv, void *context) {
    juice_config_t config;
    memset(&config, 0, sizeof(config));
    config.concurrency_mode = JUICE_CONCURRENCY_MODE_THREAD;
    config.bind_address = bind;
    config.stun_server_host = stun;
    config.stun_server_port = port;
    config.cb_state_changed = state;
    config.cb_gathering_done = done;
    config.cb_recv = recv;
    config.user_ptr = context;
    return juice_create(&config);
}

/* Called once before any agent starts. Never print SDP/credentials. */
void phantom_ice_quiet(void) { juice_set_log_level(JUICE_LOG_LEVEL_NONE); }
