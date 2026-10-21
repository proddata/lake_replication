/* Experimental Linux process ownership, shared by both launcher variants. */
static pid_t PrototypeSupervisorPid = 0;
static volatile sig_atomic_t PrototypeWorkerTerminated = false;
static MemoryContext PrototypeHandleContext = NULL;

static void
PrototypeWorkerSigterm(SIGNAL_ARGS)
{
    int saved_errno = errno;

    PrototypeWorkerTerminated = true;
    SetLatch(MyLatch);
    errno = saved_errno;
}

static void
PrototypeWorkerSigchld(SIGNAL_ARGS)
{
    int saved_errno = errno;

    /* Reap in ordinary control flow, never in the signal handler. */
    SetLatch(MyLatch);
    errno = saved_errno;
}

static pid_t
PrototypeWaitSupervisor(pid_t child, int *status)
{
    for (;;)
    {
        pid_t waited;

        /* Reset before checking waitpid/termination to avoid lost wakeups. */
        ResetLatch(MyLatch);
        if (PrototypeWorkerTerminated)
            proc_exit(0); /* before_shmem_exit owns cancellation and reaping. */
        CHECK_FOR_INTERRUPTS();
        waited = waitpid(child, status, WNOHANG);
        if (waited > 0 || (waited < 0 && errno != EINTR))
            return waited;
        if (waited == 0)
            (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, 0);
    }
}

static void
PrototypeStopSupervisor(int code, Datum arg)
{
    int status;
    int attempt;
    pid_t child = PrototypeSupervisorPid;

    (void) code;
    (void) arg;
    if (child <= 0)
        return;
    PrototypeSupervisorPid = 0;
    /* All Rust descendants inherit this private group. Resume stopped children
     * so TERM can work. Poll only during bounded shutdown, never steady state. */
    kill(-child, SIGTERM);
    kill(-child, SIGCONT);
    for (attempt = 0; attempt < 100; attempt++)
    {
        pid_t reaped;

        do
        {
            reaped = waitpid(-1, &status, WNOHANG);
        } while (reaped > 0);
        if (reaped < 0 && errno == ECHILD)
            return;
        pg_usleep(10000L);
    }
    kill(-child, SIGKILL);
    for (;;)
    {
        if (waitpid(-1, &status, 0) > 0)
            continue;
        if (errno != EINTR)
            break;
    }
}
