"""Bounded accounting and fault expectations for packaged soak runs."""
from collections import deque

FAULTS = ('worker_crash', 'storage_outage', 'destination_restart', 'backlog')


def qualifies_soak(status, requested_seconds, elapsed_seconds):
    return status == 'passed' and requested_seconds >= 86400 and elapsed_seconds >= requested_seconds


def schedule(duration):
    cycle = min(duration, 3600)
    return [(start + cycle * (index + 1) / 5, name)
            for start in range(0, duration, cycle)
            for index, name in enumerate(FAULTS)
            if start + cycle * (index + 1) / 5 < duration]


class Progress:
    def __init__(self):
        self.produced = self.observed = self.completed = 0
        self.pending = deque()
        self.max_visibility_seconds = 0
        self.buckets = {str(limit): 0 for limit in (1, 2, 5, 10, 30, 60)}
        self.buckets['over_60'] = 0

    def commit(self, sequence, when):
        if sequence != self.produced + 1:
            raise ValueError('source commit sequence is not contiguous')
        if len(self.pending) >= 10000:
            raise ValueError('observer pending-commit accounting limit exceeded')
        self.produced = sequence
        self.pending.append((sequence, when))

    def observe(self, sequence, when):
        if not self.observed <= sequence <= self.produced:
            raise ValueError('destination progress regressed or exceeded committed source progress')
        self.observed = sequence
        while self.pending and self.pending[0][0] <= sequence:
            _, committed = self.pending.popleft()
            delay = max(0, when - committed)
            self.completed += 1
            self.max_visibility_seconds = max(self.max_visibility_seconds, delay)
            key = next((key for key in self.buckets if key != 'over_60' and delay <= float(key)), 'over_60')
            self.buckets[key] += 1
        return dict(produced=self.produced, observed=self.observed, pending=len(self.pending),
                    oldest_pending_seconds=max(0, when - self.pending[0][1]) if self.pending else 0)


def validate_events(events, expected_deaths):
    deaths = {name: 0 for name in ('source', 'destination', 'pgduck', 'storage')}
    for event in events:
        action = event.get('Action', event.get('status'))
        service = event.get('Actor', {}).get('Attributes', {}).get('com.docker.compose.service')
        if service not in deaths:
            continue
        if action == 'oom':
            raise ValueError('container OOM: ' + service)
        if action == 'die':
            deaths[service] += 1
    if deaths != expected_deaths:
        raise ValueError('unexpected container exits: ' + str(deaths))
    return deaths
