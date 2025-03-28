<?php

return [

    /*
    | Maximum allowed size of a cached file in bytes. Set to -1 to allow any size.
    */
    'max_file_size' => env('FILE_CACHE_MAX_FILE_SIZE', -1),

    /*
    | Maximum age in minutes of an file in the cache. Older files are pruned.
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
     | Maximum number of attempts to set a lock on a file
     | Default: 3
     */
    'lock_max_attempts' => env('FILE_CACHE_LOCK_MAX_ATTEMPTS', 3),

    /*
     | Timeout to wait for a lock on a file to be released in seconds.
     | Default: 0 (indefinitely)
     */
    'lock_wait_timeout' => env('FILE_CACHE_LOCK_WAIT_TIMEOUT', 0),

    /*
     | Total connection timeout when reading remote files in seconds. If
     | loading the file takes longer than this, it will fail. Set to 0 to wait indefinitely.
     | Default: 0 (indefinitely)
     */
    'timeout' => env('FILE_CACHE_TIMEOUT', 0),

    /*
     | Timeout to initiate a connection to load a remote file in seconds. If
     | it takes longer, it will fail. Set to 0 to wait indefinitely.
     | Default: 30 seconds
     */
    'connect_timeout' => env('FILE_CACHE_CONNECT_TIMEOUT', 30.0),

    /*
     | Timeout for reading a stream of a remote file in seconds. If it takes
     | longer, it will fail. Set to -1 to wait indefinitely.
     | Default: 30 seconds
     */
    'read_timeout' => env('FILE_CACHE_READ_TIMEOUT', 30.0),

    /*
     | Interval for the scheduled task to prune the file cache.
     */
    'prune_interval' => '*/5 * * * *', // Every five minutes

    /*
     | Allowed MIME types for cached files. Fetching of files with any other type fails.
     | This is especially useful for files from a remote source. Leave empty to allow all
     | types.
     */
    'mime_types' => [],

];
