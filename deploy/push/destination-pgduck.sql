CREATE SECRET push_s3 (
    TYPE s3,
    SCOPE 's3://lake-push',
    KEY_ID 'lake-destination-publish',
    SECRET 'destination-publish-fixture-only',
    ENDPOINT 'storage:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
