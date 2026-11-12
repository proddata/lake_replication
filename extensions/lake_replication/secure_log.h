#ifndef LAKE_REPLICATION_SECURE_LOG_H
#define LAKE_REPLICATION_SECURE_LOG_H

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/* Hold the checked directory open so a rename cannot redirect file creation.
 * Both it and the log must belong exclusively to the PostgreSQL OS user.
 * O_NONBLOCK prevents an existing FIFO from hanging the launcher before fstat.
 */
static int
lake_open_private_log(const char *directory, const char *name)
{
    struct stat metadata;
    int directory_fd;
    int log_fd;
    int saved_errno;

    directory_fd = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (directory_fd < 0)
        return -1;
    if (fstat(directory_fd, &metadata) < 0)
        goto directory_error;
    if (!S_ISDIR(metadata.st_mode) || metadata.st_uid != geteuid() ||
        (metadata.st_mode & 0077) != 0)
    {
        errno = EACCES;
        goto directory_error;
    }

    log_fd = openat(directory_fd, name,
                    O_CREAT | O_WRONLY | O_APPEND | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC,
                    0600);
    saved_errno = errno;
    close(directory_fd);
    errno = saved_errno;
    if (log_fd < 0)
        return -1;
    if (fstat(log_fd, &metadata) < 0)
        goto file_error;
    if (!S_ISREG(metadata.st_mode) || metadata.st_uid != geteuid() ||
        metadata.st_nlink != 1 || (metadata.st_mode & 0077) != 0)
    {
        errno = EACCES;
        goto file_error;
    }
    return log_fd;

file_error:
    saved_errno = errno;
    close(log_fd);
    errno = saved_errno;
    return -1;

directory_error:
    saved_errno = errno;
    close(directory_fd);
    errno = saved_errno;
    return -1;
}

#endif
