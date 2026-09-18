CREATE SECRET poc_s3 (
    TYPE s3,
    SCOPE 's3://lake-poc',
    KEY_ID 'lake-poc',
    SECRET 'local-poc-only',
    ENDPOINT 'storage:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
