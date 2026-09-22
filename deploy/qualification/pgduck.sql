CREATE SECRET qualification_s3 (
    TYPE s3,
    SCOPE 's3://lake-qualification',
    KEY_ID 'lake-qualification',
    SECRET 'qualification-object-secret',
    ENDPOINT 'storage:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
