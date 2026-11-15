#!/usr/bin/env python3
"""Exported logical-slot snapshot under concurrent committed mutations."""
import sys
from snapshot import SnapshotProbe

class OnlineSnapshotProbe(SnapshotProbe):
    online=True
    def __init__(self):
        super().__init__()
        self.evidence.update(scope='bounded online slot-exported baseline with concurrent mutations',
            unqualified=['unbounded/chunked snapshots','ordered DDL','scoped source roles','physical cleanup','production API'])

if __name__=='__main__':sys.exit(OnlineSnapshotProbe().run())
