"""Diagnostic visibility comparison; fixture oracle is not production telemetry."""

# One aggregate replaces the ordinary publication-count query when opted in.
# Source commit age crosses server clocks; local timing validation is required.
SQL = """SELECT count(*) FILTER (WHERE state='published'),
 extract(epoch FROM clock_timestamp()),
 count(*) FILTER (WHERE state<>'published'),
 extract(epoch FROM clock_timestamp()-min(staged_at) FILTER (WHERE state<>'published')),
 extract(epoch FROM clock_timestamp()-min(commit_time) FILTER (WHERE state<>'published'))
 FROM lake_replication.transactions"""


def sample(values, known_commits):
    published, epoch, pending, staged_age, commit_age = values
    published, pending, epoch = int(published), int(pending), float(epoch)
    # Publication order follows this fixture's single ordered source per stream.
    # The snapshot of known commits is taken before querying the destination.
    # A newer destination observation can therefore publish beyond this prefix.
    outstanding = [row for row in known_commits[published:] if row['commit_epoch'] <= epoch]
    age = max(0, epoch-outstanding[0]['commit_epoch']) if outstanding else 0.0
    return dict(published_transactions=published, observed_epoch=epoch,
        destination_pending_transactions=pending,
        oldest_pending_staged_age_seconds=float(staged_age) if staged_age else None,
        oldest_pending_source_commit_age_seconds=float(commit_age) if commit_age else None,
        fixture_known_commits=len(known_commits), fixture_unpublished_transactions=len(outstanding),
        fixture_oldest_unpublished_age_seconds=age,
        fixture_pending_not_in_destination=max(0,len(outstanding)-pending),
        receiver_health='unknown',
        scope='Fixture-prefix comparison only; empty destination queue does not establish receiver health')
