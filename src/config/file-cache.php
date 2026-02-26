<?php

return [

    /*
    | Maximum allowed size of a cached file in bytes. Set to -1 to allow any size.
    */
    'max_file_size' => env('FILE_CACHE_MAX_FILE_SIZE', -1),

    /*
    | Maximum age in minutes of a file in the cache. Older files are pruned.
    */
    'max_age' => env('FILE_CACHE_MAX_AGE', 60),

    /*
    | Maximum size (soft limit) of the file cache in bytes. If the cache exceeds
    | this size, old files are pruned.
    */
    'max_size' => env('FILE_CACHE_MAX_SIZE', 1E+9), // 1 GB

    /*
    | Directory to use for the file cache.
    */
    'path' => storage_path('framework/cache/files'),

    /*
     | Maximum number of attempts to set a lock on a file.
     | Must be at least 1.
     | Default: 3
     */
    'lock_max_attempts' => env('FILE_CACHE_LOCK_MAX_ATTEMPTS', 3),

    /*
     | Timeout to wait for a lock on a file to be released in seconds.
     | Set to -1 to wait indefinitely.
     | Default: -1 (indefinitely)
     */
    'lock_wait_timeout' => env('FILE_CACHE_LOCK_WAIT_TIMEOUT', -1),

    /*
     | Total connection timeout when reading remote files in seconds.
     | If loading the file takes longer than this, it will fail.
     | Set to -1 to wait indefinitely.
     | Default: -1 (indefinitely)
     */
    'timeout' => env('FILE_CACHE_TIMEOUT', -1),

    /*
     | Timeout to initiate a connection to load a remote file in seconds.
     | If it takes longer, it will fail. Set to -1 to wait indefinitely.
     | Default: 30 seconds
     */
    'connect_timeout' => env('FILE_CACHE_CONNECT_TIMEOUT', 30.0),

    /*
     | Timeout for reading a stream of a remote file in seconds.
     | If it takes longer, it will fail. Set to -1 to wait indefinitely.
     | Default: 30 seconds
     */
    'read_timeout' => env('FILE_CACHE_READ_TIMEOUT', 30.0),

    /*
     | Interval for the scheduled task to prune the file cache.
     */
    'prune_interval' => env('FILE_CACHE_PRUNE_INTERVAL', '*/5 * * * *'),

    /*
     | Timeout for the prune operation in seconds.
     | If pruning takes longer than this, it will stop early.
     | Set to -1 for no timeout.
     | Default: 300 seconds (5 minutes)
     */
    'prune_timeout' => env('FILE_CACHE_PRUNE_TIMEOUT', 300),

    /*
     | Allowed MIME types for cached files. Fetching of files with any other type fails.
     | This is especially useful for files from a remote source. Leave empty to allow all
     | types.
     */
    'mime_types' => [],

    /*
     | Allowed hosts for remote file fetching. This is a security feature to prevent
     | SSRF (Server-Side Request Forgery) attacks. Set to null to allow all hosts,
     | or provide an array of allowed hostnames.
     | Example: ['example.com', 'cdn.example.com', '*.trusted-domain.com']
     | Wildcards (*) are supported at the beginning of hostnames.
     | Default: null (all hosts allowed)
     */
    'allowed_hosts' => env('FILE_CACHE_ALLOWED_HOSTS', null),

    /*
     | Number of retry attempts for failed HTTP requests.
     | Set to 0 to disable retries.
     | Default: 0
     */
    'http_retries' => env('FILE_CACHE_HTTP_RETRIES', 0),

    /*
     | Delay between HTTP retry attempts in milliseconds.
     | Default: 100
     */
    'http_retry_delay' => env('FILE_CACHE_HTTP_RETRY_DELAY', 100),

    /*
     | Timeout to wait for lifecycle lock acquisition in seconds.
     | This lock coordinates batch/batchOnce with prune/clear operations.
     | Set to -1 to wait indefinitely.
     | Default: 30 seconds
     */
    'lifecycle_lock_timeout' => env('FILE_CACHE_LIFECYCLE_LOCK_TIMEOUT', 30.0),

    /*
     | Maximum number of files to process in a single chunk during
     | batch() and batchOnce() to avoid file descriptor exhaustion.
     | Set to -1 for no limit.
     | Default: 100
     */
    'batch_chunk_size' => env('FILE_CACHE_BATCH_CHUNK_SIZE', 100),

];
