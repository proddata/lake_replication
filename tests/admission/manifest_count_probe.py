#!/usr/bin/env python3
"""Compile the candidate's actual count-reader helper against cached libavro."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from commit_profile_overlay import patch


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='pg-lake-qualification-base:8180647d4535')
    args=parser.parse_args();root=Path(__file__).resolve().parents[2]
    path='pg_lake_iceberg/src/iceberg/read_manifest.c'
    text=patch(path,(root/'vendor/pg_lake'/path).read_text())
    begin=text.index('static bool\nLakeDeletedCountKnown(');end=text.index('\n\nstatic void\nReadIcebergManifestFromAvro(',begin)
    helper=text[begin:end]
    source='''#include <avro.h>
#include <assert.h>
#include <stdbool.h>
#include <string.h>
'''+helper+r'''
int main(void) {
    const char *schemas[]={
        "{\"type\":\"record\",\"name\":\"plain\",\"fields\":[{\"name\":\"deleted_files_count\",\"type\":\"int\"}]}",
        "{\"type\":\"record\",\"name\":\"nullable\",\"fields\":[{\"name\":\"deleted_files_count\",\"type\":[\"null\",\"int\"]}]}"
    };
    for (int index=0;index<2;index++) {
        avro_schema_t schema;
        assert(avro_schema_from_json_length(schemas[index],strlen(schemas[index]),&schema)==0);
        avro_value_iface_t *iface=avro_generic_class_from_schema(schema);
        avro_value_t record,field,branch;
        assert(avro_generic_value_new(iface,&record)==0);
        assert(avro_value_get_by_name(&record,"deleted_files_count",&field,NULL)==0);
        if (index==0) { assert(avro_value_set_int(&field,0)==0); }
        else {
            assert(avro_value_set_branch(&field,0,&branch)==0);
            assert(avro_value_set_null(&branch)==0);
            assert(!LakeDeletedCountKnown(&record,"deleted_files_count"));
            assert(avro_value_set_branch(&field,1,&branch)==0);
            assert(avro_value_set_int(&branch,0)==0);
        }
        assert(LakeDeletedCountKnown(&record,"deleted_files_count"));
        assert(!LakeDeletedCountKnown(&record,"missing_count"));
        avro_value_decref(&record);avro_value_iface_decref(iface);avro_schema_decref(schema);
    }
    return 0;
}
'''
    with tempfile.TemporaryDirectory(prefix='lake-count-test-') as directory:
        file=Path(directory)/'count-test.c';file.write_text(source)
        subprocess.run(['docker','run','--rm','--mount',f'type=bind,src={file},target=/tmp/count-test.c,readonly',
            '--entrypoint','sh',args.image,'-ec','cc -I/usr/include/postgresql /tmp/count-test.c -lavro -o /tmp/count-test && /tmp/count-test'],check=True)
    image=subprocess.check_output(['docker','image','inspect','--format','{{.Id}}',args.image],text=True).strip()
    print(json.dumps(dict(status='passed',scope=__doc__,image_id=image,
        helper_sha256=hashlib.sha256(helper.encode()).hexdigest(),test_source_sha256=hashlib.sha256(source.encode()).hexdigest(),
        cases=['plain zero is known','nullable null is unknown','nullable zero is known','missing count is unknown'])))


if __name__=='__main__':main()
