CREATE SECRET lab_s3 (
    TYPE s3,
    SCOPE 's3://lake-lab',
    KEY_ID 'lake-lab',
    SECRET 'local-lab-only',
    ENDPOINT 'storage:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
