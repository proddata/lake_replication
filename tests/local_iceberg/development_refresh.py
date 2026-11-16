#!/usr/bin/env python3
"""Current SQL refresh regression on cached native runtime; not archive qualification."""
from pathlib import Path
import subprocess
import hashlib
import sys
from types import SimpleNamespace
from probe import LocalIcebergProbe,ROOT
from session import Session
sys.path.insert(0,str(ROOT/'tests/performance'))
from update_filter_probe import check_types,check_edges,check_upgrade


class RefreshProbe(LocalIcebergProbe):
    def provenance(self):
        super().provenance()
        for name in ['deploy/install/build-current-beta5-refresh.sh','deploy/install/Dockerfile','tests/performance/update_filter_probe.py']:
            self.evidence['files_sha256'][name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest()

    def exercise(self):
        self.evidence.update(scope=__doc__)
        def client(service,database):
            args=self.sql_args();args[-1]=database
            return Session(args)
        q=SimpleNamespace(check=self.check,
            sql=lambda service,database,statement:self.sql(statement),
            sql_result=lambda service,database,statement,**kwargs:self.sql(statement,required=False))
        h=SimpleNamespace(q=q,c=SimpleNamespace(Psql=client),client=Session(self.sql_args()))
        try:
            patch=subprocess.check_output(['sh',str(ROOT/'deploy/install/build-current-beta5-refresh.sh')],text=True)
            check_types(h);check_edges(h);check_upgrade(h,patch)
        finally:h.client.close()


if __name__=='__main__':sys.exit(RefreshProbe().run())
