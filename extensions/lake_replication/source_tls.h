#ifndef LAKE_REPLICATION_SOURCE_TLS_H
#define LAKE_REPLICATION_SOURCE_TLS_H

#include <arpa/inet.h>
#include <libpq-fe.h>
#include <string.h>

typedef struct LakeSourceTls
{
    PQconninfoOption *options;
    const char *mode;
    const char *root;
    const char *host;
} LakeSourceTls;

static int
lake_source_is_loopback(const char *host)
{
    struct in_addr ipv4;
    struct in6_addr ipv6;

    return strcmp(host, "localhost") == 0 ||
        (inet_pton(AF_INET, host, &ipv4) == 1 && (ntohl(ipv4.s_addr) >> 24) == 127) ||
        (inet_pton(AF_INET6, host, &ipv6) == 1 && IN6_IS_ADDR_LOOPBACK(&ipv6));
}

/* Values borrow from options, which the caller frees on success or failure.
 * Do not include libpq's parse diagnostics: they may contain credentials.
 */
static const char *
lake_source_tls_policy(const char *conninfo, LakeSourceTls *policy)
{
    PQconninfoOption *option;
    char *error = NULL;
    const char *hostaddr = NULL;
    int loopback;

    memset(policy, 0, sizeof(*policy));
    policy->options = PQconninfoParse(conninfo, &error);
    if (policy->options == NULL)
    {
        if (error != NULL)
            PQfreemem(error);
        return "invalid source CONNECTION string";
    }
    for (option = policy->options; option->keyword != NULL; option++)
    {
        if (option->val == NULL || option->val[0] == '\0')
            continue;
        if (strcmp(option->keyword, "host") == 0)
            policy->host = option->val;
        else if (strcmp(option->keyword, "sslmode") == 0)
            policy->mode = option->val;
        else if (strcmp(option->keyword, "sslrootcert") == 0)
            policy->root = option->val;
        else if (strcmp(option->keyword, "hostaddr") == 0)
            hostaddr = option->val;
        else if (strcmp(option->keyword, "service") == 0)
            return "source CONNECTION must specify its settings directly, without service files";
    }
    if (policy->host == NULL || policy->host[0] == '/' ||
        policy->host[0] == '@' || strchr(policy->host, ',') != NULL)
        return "source CONNECTION requires one explicit TCP host";

    if (hostaddr != NULL && strchr(hostaddr, ',') != NULL)
        return "source CONNECTION requires one TCP hostaddr";
    loopback = lake_source_is_loopback(policy->host) &&
        (hostaddr == NULL || lake_source_is_loopback(hostaddr));
    if (policy->mode == NULL)
        policy->mode = loopback ? "disable" : "verify-full";
    if (strcmp(policy->mode, "disable") != 0 && strcmp(policy->mode, "verify-full") != 0)
        return "source sslmode must be verify-full or explicitly disable";
    if (strcmp(policy->mode, "verify-full") == 0 &&
        (policy->root == NULL || policy->root[0] != '/'))
        return "source verify-full requires an explicit absolute sslrootcert file path";
    return NULL;
}

#endif
