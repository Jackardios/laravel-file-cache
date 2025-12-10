# File Cache


Fetch and cache files from local filesystem, cloud storage or public webservers in Laravel.

The file cache is specifically designed for use in concurrent processing with multiple parallel queue workers.

[![Tests](https://github.com/jackardios/laravel-file-cache/actions/workflows/tests.yml/badge.svg)](https://github.com/jackardios/laravel-file-cache/actions/workflows/tests.yml)

## Requirements

- PHP ^8.1
- Laravel ^10.0 || ^11.0 || ^12.0

## Installation

```
composer require jackardios/laravel-file-cache
```

### Laravel

The service provider and `FileCache` facade are auto-discovered by Laravel.

### Publishing Configuration

```bash
php artisan vendor:publish --provider="Jackardios\FileCache\FileCacheServiceProvider" --tag="config"
```

## Usage

Take a look at the [`FileCache`](src/Contracts/FileCache.php) contract to see the public API of the file cache. Example:

```php
use FileCache;
use Jackardios\FileCache\GenericFile;

// Implements Jackardios\FileCache\Contracts\File.
$file = new GenericFile('https://example.com/images/image.jpg');

FileCache::get($file, function ($file, $path) {
    // do stuff
});
```

If the file URL specifies another protocol than `http` or `https` (e.g. `mydisk://images/image.jpg`), the file cache looks for the file in the appropriate storage disk configured at `filesystems.disks`. You can not use a local file path as URL (e.g. `/vol/images/image.jpg`). Instead, configure a storage disk with the `local` driver.

### Batch Processing

Process multiple files at once while maintaining locks to prevent pruning during processing:

```php
use FileCache;
use Jackardios\FileCache\GenericFile;

$files = [
    new GenericFile('https://example.com/image1.jpg'),
    new GenericFile('https://example.com/image2.jpg'),
];

FileCache::batch($files, function ($files, $paths) {
    // $paths contains cached file paths in the same order as $files
    foreach ($paths as $path) {
        // process each file
    }
});
```

### One-time Files

Use `getOnce()` or `batchOnce()` to automatically delete cached files after processing:

```php
FileCache::getOnce($file, function ($file, $path) {
    // File will be deleted after callback completes
});
```

### Check File Existence

```php
$exists = FileCache::exists($file);
```

## Configuration

The file cache comes with a sensible default configuration. You can override it in the `file-cache` namespace or with environment variables.

### file-cache.max_file_size

Default: `-1` (any size)
Environment: `FILE_CACHE_MAX_FILE_SIZE`

Maximum allowed size of a cached file in bytes. Set to `-1` to allow any size.

### file-cache.max_age

Default: `60`
Environment: `FILE_CACHE_MAX_AGE`

Maximum age in minutes of a file in the cache. Older files are pruned.

### file-cache.max_size

Default: `1E+9` (1 GB)
Environment: `FILE_CACHE_MAX_SIZE`

Maximum size (soft limit) of the file cache in bytes. If the cache exceeds this size, old files are pruned.

### file-cache.path

Default: `'storage/framework/cache/files'`

Directory to use for the file cache.

### file-cache.timeout

Default: `-1` (indefinitely)
Environment: `FILE_CACHE_TIMEOUT`

Total connection timeout when reading remote files in seconds. If loading the file takes longer than this, it will fail. Set to `-1` to wait indefinitely.

### file-cache.connect_timeout

Default: `30` (30 seconds)
Environment: `FILE_CACHE_CONNECT_TIMEOUT`

Timeout to initiate a connection to load a remote file in seconds. If it takes longer, it will fail. Set to `-1` to wait indefinitely.

### file-cache.read_timeout

Default: `30` (30 seconds)
Environment: `FILE_CACHE_READ_TIMEOUT`

Timeout for reading a stream of a remote file in seconds. If it takes longer, it will fail. Set to `-1` to wait indefinitely.

### file-cache.prune_interval

Default `'*/5 * * * *'` (every five minutes)

Interval for the scheduled task to prune the file cache.

### file-cache.prune_timeout

Default: `300` (5 minutes)
Environment: `FILE_CACHE_PRUNE_TIMEOUT`

Timeout for the prune operation in seconds. If pruning takes longer than this, it will stop early. Set to `-1` for no timeout.

### file-cache.mime_types

Default: `[]` (allow all types)

Array of allowed MIME types for cached files. Caching of files with other types will fail.

### file-cache.allowed_hosts

Default: `null` (all hosts allowed)
Environment: `FILE_CACHE_ALLOWED_HOSTS`

Allowed hosts for remote file fetching. This is a security feature to prevent SSRF (Server-Side Request Forgery) attacks. Set to `null` to allow all hosts, or provide an array of allowed hostnames.

Wildcards (`*`) are supported at the beginning of hostnames:

```php
'allowed_hosts' => ['example.com', 'cdn.example.com', '*.trusted-domain.com'],
```

For environment variable, use comma-separated values:
```
FILE_CACHE_ALLOWED_HOSTS=example.com,cdn.example.com,*.trusted-domain.com
```

### file-cache.http_retries

Default: `0` (no retries)
Environment: `FILE_CACHE_HTTP_RETRIES`

Number of retry attempts for failed HTTP requests. Client errors (4xx except 429) are not retried.

### file-cache.http_retry_delay

Default: `100` (100ms)
Environment: `FILE_CACHE_HTTP_RETRY_DELAY`

Delay between HTTP retry attempts in milliseconds.

### file-cache.batch_chunk_size

Default: `100`
Environment: `FILE_CACHE_BATCH_CHUNK_SIZE`

Maximum number of files to process in a single batch to avoid file descriptor exhaustion. Set to `-1` for no limit.

### file-cache.lock_max_attempts

Default: `3`
Environment: `FILE_CACHE_LOCK_MAX_ATTEMPTS`

Maximum number of attempts to acquire a lock on a file. Must be at least 1.

### file-cache.lock_wait_timeout

Default: `-1` (indefinitely)
Environment: `FILE_CACHE_LOCK_WAIT_TIMEOUT`

Timeout to wait for a lock on a file to be released in seconds. Set to `-1` to wait indefinitely.

## Clearing

The file cache is cleared when you call `php artisan cache:clear`.

## Testing

The `FileCache` facade provides a fake for easy testing. The fake does not actually fetch and store any files, but only executes the callback function with a faked file path.

```php
use FileCache;
use Jackardios\FileCache\GenericFile;

FileCache::fake();
$file = new GenericFile('https://example.com/image.jpg');
$path = FileCache::get($file, function ($file, $path) {
    return $path;
});

$this->assertFalse($this->app['files']->exists($path));
```

## Exceptions

The following exceptions may be thrown:

- `FileIsTooLargeException` - File exceeds configured `max_file_size`
- `FileLockedException` - File is locked when `throwOnLock` is `true`
- `HostNotAllowedException` - Host is not in the `allowed_hosts` whitelist
- `MimeTypeIsNotAllowedException` - MIME type is not in the `mime_types` whitelist
- `SourceResourceIsInvalidException` - Could not establish a valid stream resource
- `SourceResourceTimedOutException` - Stream read operation timed out
- `FailedToRetrieveFileException` - General file retrieval failure after all retries
- `InvalidConfigurationException` - Invalid configuration values

## License

MIT
