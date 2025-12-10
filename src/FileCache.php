<?php

namespace Jackardios\FileCache;

use GuzzleHttp\Exception\RequestException;
use Jackardios\FileCache\Contracts\File;
use Jackardios\FileCache\Contracts\FileCache as FileCacheContract;
use Jackardios\FileCache\Exceptions\FailedToRetrieveFileException;
use Jackardios\FileCache\Exceptions\FileIsTooLargeException;
use Jackardios\FileCache\Exceptions\FileLockedException;
use Jackardios\FileCache\Exceptions\HostNotAllowedException;
use Jackardios\FileCache\Exceptions\InvalidConfigurationException;
use Jackardios\FileCache\Exceptions\MimeTypeIsNotAllowedException;
use Jackardios\FileCache\Exceptions\SourceResourceIsInvalidException;
use Jackardios\FileCache\Exceptions\SourceResourceTimedOutException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Contracts\Filesystem\FileNotFoundException;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Filesystem\FilesystemManager;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

/**
 * The file cache.
 */
class FileCache implements FileCacheContract
{
    /**
     * Logger instance for diagnostic logging.
     */
    protected LoggerInterface $logger;

    /**
     * Create an instance.
     */
    public function __construct(
        protected array $config = [],
        protected ?Client $client = null,
        protected ?Filesystem $files = null,
        protected ?FilesystemManager $storage = null,
        ?LoggerInterface $logger = null
    ) {
        $this->config = $this->prepareConfig($config);
        $this->client = $client ?: $this->makeHttpClient();
        $this->files = $files ?: app('files');
        $this->storage = $storage ?: app('filesystem');
        $this->logger = $logger ?: new NullLogger();
    }

    /**
     * {@inheritdoc}
     *
     * @throws MimeTypeIsNotAllowedException
     * @throws FileIsTooLargeException
     */
    public function exists(File $file): bool
    {
        return $this->isRemote($file) ? $this->existsRemote($file) : $this->existsDisk($file);
    }

    /**
     * {@inheritdoc}
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileLockedException
     * @throws FailedToRetrieveFileException
     */
    public function get(File $file, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? \Closure::fromCallable([static::class, 'defaultGetCallback']);

        return $this->batch([$file], function ($files, $paths) use ($callback) {
            return call_user_func($callback, $files[0], $paths[0]);
        }, $throwOnLock);
    }

    /**
     * {@inheritdoc}
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileLockedException
     * @throws FailedToRetrieveFileException
     */
    public function getOnce(File $file, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? \Closure::fromCallable([static::class, 'defaultGetCallback']);

        return $this->batchOnce([$file], function ($files, $paths) use ($callback) {
            return call_user_func($callback, $files[0], $paths[0]);
        }, $throwOnLock);
    }

    /**
     * {@inheritdoc}
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileLockedException
     * @throws FailedToRetrieveFileException
     */
    public function batch(array $files, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? \Closure::fromCallable([static::class, 'defaultBatchCallback']);
        $chunkSize = $this->config['batch_chunk_size'];

        if ($chunkSize < 0 || count($files) <= $chunkSize) {
            return $this->processBatch($files, $callback, $throwOnLock);
        }

        // For chunked processing, we need to maintain locks across all chunks
        // to prevent prune() from deleting files before the final callback
        return $this->processBatchChunked($files, $callback, $throwOnLock, $chunkSize);
    }

    /**
     * Process files in chunks while maintaining locks across all chunks.
     *
     * @param array $files
     * @param callable $callback
     * @param bool $throwOnLock
     * @param int $chunkSize
     * @return mixed
     */
    protected function processBatchChunked(array $files, callable $callback, bool $throwOnLock, int $chunkSize)
    {
        $allRetrieved = [];
        $chunks = array_chunk($files, $chunkSize, true);

        try {
            foreach ($chunks as $chunkFiles) {
                foreach ($chunkFiles as $index => $file) {
                    $allRetrieved[$index] = $this->retrieve($file, $throwOnLock);
                }
            }

            $paths = array_map(static function ($file) {
                return $file['path'];
            }, $allRetrieved);

            return call_user_func($callback, $files, $paths);
        } finally {
            // Close all streams after callback completes
            foreach ($allRetrieved as $file) {
                if (isset($file['stream']) && is_resource($file['stream'])) {
                    fclose($file['stream']);
                }
            }
        }
    }

    /**
     * Process a batch of files (internal implementation).
     *
     * @param array $files
     * @param callable|null $callback
     * @param bool $throwOnLock
     * @param bool $deleteAfter Whether to delete files after callback execution
     * @return mixed
     */
    protected function processBatch(array $files, ?callable $callback, bool $throwOnLock, bool $deleteAfter = false)
    {
        $callback = $callback ?? \Closure::fromCallable([static::class, 'defaultBatchCallback']);

        $retrieved = [];
        try {
            // Use explicit loop instead of array_map to properly handle exceptions
            // and close already-opened streams if an exception occurs mid-batch
            foreach ($files as $index => $file) {
                $retrieved[$index] = $this->retrieve($file, $throwOnLock);
            }

            $paths = array_map(static function ($file) {
                return $file['path'];
            }, $retrieved);

            return call_user_func($callback, $files, $paths);
        } finally {
            // Close streams and optionally delete files
            foreach ($retrieved as $file) {
                if (!isset($file['stream']) || !is_resource($file['stream'])) {
                    continue;
                }

                $path = $file['path'];

                // Must close the stream BEFORE attempting delete
                // because we can't upgrade LOCK_SH to LOCK_EX on the same handle
                fclose($file['stream']);

                if ($deleteAfter) {
                    // Re-open file to acquire exclusive lock for deletion
                    $handle = @fopen($path, 'rb');
                    if ($handle !== false) {
                        if (flock($handle, LOCK_EX | LOCK_NB)) {
                            $this->files->delete($path);
                        }
                        fclose($handle);
                    }
                }
            }
        }
    }

    /**
     * {@inheritdoc}
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileLockedException
     * @throws FailedToRetrieveFileException
     */
    public function batchOnce(array $files, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? \Closure::fromCallable([static::class, 'defaultBatchCallback']);

        return $this->processBatch($files, $callback, $throwOnLock, deleteAfter: true);
    }

    /**
     * {@inheritdoc}
     *
     * @return array{deleted: int, remaining: int, total_size: int} Statistics about pruning operation
     */
    public function prune(): array
    {
        $stats = ['deleted' => 0, 'remaining' => 0, 'total_size' => 0];

        if (!$this->files->exists($this->config['path'])) {
            return $stats;
        }

        $startTime = time();
        $timeout = $this->config['prune_timeout'];
        $now = time();
        $allowedAge = $this->config['max_age'] * 60;
        $allowedSize = $this->config['max_size'];

        // Collect all files with their metadata in a single pass
        $fileInfos = [];
        $files = Finder::create()
            ->files()
            ->ignoreDotFiles(true)
            ->in($this->config['path'])
            ->getIterator();

        foreach ($files as $file) {
            if ($this->isPruneTimedOut($startTime, $timeout, 'file collection')) {
                $this->logger->warning('Prune operation timed out during file collection');
                return $stats;
            }

            try {
                $fileInfos[] = [
                    'file' => $file,
                    'atime' => $file->getATime(),
                    'size' => $file->getSize(),
                ];
            } catch (RuntimeException $e) {
                // File might have been deleted, skip it
                continue;
            }
        }

        // Sort by access time (oldest first) for both age and size-based pruning
        usort($fileInfos, static fn($a, $b) => $a['atime'] <=> $b['atime']);

        $totalSize = 0;
        $remainingFiles = [];

        // First pass: prune by age, collect remaining files
        foreach ($fileInfos as $info) {
            if ($this->isPruneTimedOut($startTime, $timeout, 'age-based pruning')) {
                return $stats;
            }

            $isExpired = ($now - $info['atime']) > $allowedAge;

            if ($isExpired && $this->delete($info['file'])) {
                $stats['deleted']++;
                continue;
            }

            // File was not deleted (either not expired or locked)
            $totalSize += $info['size'];
            $remainingFiles[] = $info;
        }

        // Second pass: prune by size if needed (files already sorted by atime)
        if ($totalSize > $allowedSize) {
            foreach ($remainingFiles as $info) {
                if ($totalSize <= $allowedSize) {
                    break;
                }

                if ($this->isPruneTimedOut($startTime, $timeout, 'size-based pruning', $totalSize - $allowedSize)) {
                    break;
                }

                if ($this->delete($info['file'])) {
                    $totalSize -= $info['size'];
                    $stats['deleted']++;
                } else {
                    $stats['remaining']++;
                }
            }
        }

        $stats['total_size'] = $totalSize;
        $stats['remaining'] = count($remainingFiles) - ($stats['deleted'] - (count($fileInfos) - count($remainingFiles)));

        // Recalculate remaining properly
        $stats['remaining'] = 0;
        foreach ($remainingFiles as $info) {
            if ($this->files->exists($info['file']->getRealPath())) {
                $stats['remaining']++;
            }
        }

        return $stats;
    }

    /**
     * Check if prune operation has timed out.
     *
     * @param int $startTime Start time of the prune operation
     * @param int $timeout Timeout in seconds (0 or negative = no timeout)
     * @param string $phase Current pruning phase for logging
     * @param int|null $remainingSize Remaining size to prune (for logging)
     * @return bool True if timed out
     */
    protected function isPruneTimedOut(int $startTime, int $timeout, string $phase, ?int $remainingSize = null): bool
    {
        if ($timeout <= 0) {
            return false;
        }

        $elapsed = time() - $startTime;
        if ($elapsed <= $timeout) {
            return false;
        }

        $context = [
            'timeout' => $timeout,
            'elapsed' => $elapsed,
        ];

        if ($remainingSize !== null) {
            $context['remaining_size'] = $remainingSize;
        }

        $this->logger->warning("Prune operation timed out during {$phase}", $context);
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function clear(): void
    {
        if (!$this->files->exists($this->config['path'])) {
            return;
        }

        $files = Finder::create()
            ->files()
            ->ignoreDotFiles(true)
            ->in($this->config['path'])
            ->getIterator();

        foreach ($files as $file) {
            $this->delete($file);
        }
    }

    /**
     * Check for existence of a remote file.
     *
     * @throws MimeTypeIsNotAllowedException
     * @throws FileIsTooLargeException
     * @throws HostNotAllowedException
     */
    protected function existsRemote(File $file): bool
    {
        $this->validateHost($file->getUrl());
        $response = $this->client->head($this->encodeUrl($file->getUrl()));
        $code = $response->getStatusCode();

        if ($code < 200 || $code >= 300) {
            return false;
        }

        if (!empty($this->config['mime_types'])) {
            $type = $response->getHeaderLine('content-type');
            $type = trim(explode(';', $type)[0]);
            if ($type && !in_array($type, $this->config['mime_types'], true)) {
                throw MimeTypeIsNotAllowedException::create($type);
            }
        }

        $maxBytes = $this->config['max_file_size'];
        $contentLength = $response->getHeaderLine('content-length');
        $contentBytes = is_numeric($contentLength) ? (int) $contentLength : null;

        if ($maxBytes >= 0 && $contentBytes !== null && $contentBytes > $maxBytes) {
            throw FileIsTooLargeException::create($maxBytes);
        }

        return true;
    }

    /**
     * Check for existence of a file from a storage disk.
     *
     * @throws MimeTypeIsNotAllowedException
     * @throws FileIsTooLargeException
     */
    protected function existsDisk(File $file): bool
    {
        $urlWithoutProtocol = $this->splitByProtocol($file->getUrl())[1] ?? null;
        if ($urlWithoutProtocol === null) {
            return false;
        }

        $disk = $this->getDisk($file);
        $exists = $disk->exists($urlWithoutProtocol);

        if (!$exists) {
            return false;
        }

        if (!empty($this->config['mime_types'])) {
            $type = $disk->mimeType($urlWithoutProtocol);
            if (!in_array($type, $this->config['mime_types'], true)) {
                throw MimeTypeIsNotAllowedException::create($type);
            }
        }

        $maxBytes = $this->config['max_file_size'];

        if ($maxBytes >= 0) {
            $size = $disk->size($urlWithoutProtocol);
            if ($size > $maxBytes) {
                throw FileIsTooLargeException::create($maxBytes);
            }
        }

        return true;
    }

    /**
     * Delete a cached file if it is not used.
     *
     * @param SplFileInfo $file
     *
     * @return bool If the file has been deleted.
     */
    protected function delete(SplFileInfo $file): bool
    {
        $fileStream = null;
        $deleted = false;
        $filePath = $file->getRealPath();

        if ($filePath === false) {
            return true;
        }

        try {
            $fileStream = @fopen($filePath, 'rb');
            if ($fileStream === false) {
                // Cannot open file - it's already deleted or inaccessible
                // Consider it successfully "deleted" for pruning purposes
                return !file_exists($filePath);
            }

            // Only delete the file if it is not currently used. Else move on.
            // Use non-blocking to avoid deadlocks
            if (flock($fileStream, LOCK_EX | LOCK_NB)) {
                $this->files->delete($filePath);
                $deleted = true;
            }
        } catch (Exception $e) {
            // Ignore exceptions when deleting cache files
            return false;
        } finally {
            if (is_resource($fileStream)) {
                fclose($fileStream);
            }
        }

        return $deleted;
    }

    /**
     * Cache a remote or cloud storage file if it is not cached and get the path to
     * the cached file. If the file is local, nothing will be done and the path to the
     * local file will be returned.
     *
     * @return array Containing the 'path' to the file and the file 'stream'. Close the
     * stream when finished.
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileLockedException
     * @throws FailedToRetrieveFileException
     */
    protected function retrieve(File $file, bool $throwOnLock = false): array
    {
        $this->ensurePathExists();
        $cachedPath = $this->getCachedPath($file);
        $attempt = 0;

        while ($attempt < $this->config['lock_max_attempts']) {
            $attempt++;

            // This will return false if the file already exists. Else it will create it in
            // read and write mode.
            $cachedFileStream = @fopen($cachedPath, 'xb+');

            if ($cachedFileStream === false) {
                // The file exists, try to get the file stream in read mode.
                $cachedFileStream = @fopen($cachedPath, 'rb');

                // If we can't open the file, it might be in a bad state or being deleted
                if ($cachedFileStream === false) {
                    // Wait a moment before retrying
                    usleep(100000); // 100ms
                    continue;
                }

                if ($throwOnLock && !flock($cachedFileStream, LOCK_SH | LOCK_NB)) {
                    fclose($cachedFileStream);
                    throw FileLockedException::create();
                }

                // Wait for any LOCK_EX that is set if the file is currently written.
                $lockAcquired = false;
                $startTime = microtime(true);
                $lockTimeout = $this->config['lock_wait_timeout'];
                $hasLockTimeout = $lockTimeout >= 0;

                while (!$lockAcquired) {
                    $lockAcquired = flock($cachedFileStream, LOCK_SH | LOCK_NB);

                    // Check timeout (-1 means wait indefinitely)
                    if ($hasLockTimeout && (microtime(true) - $startTime) >= $lockTimeout) {
                        break;
                    }

                    if (!$lockAcquired) {
                        usleep(50000); // 50ms
                    }
                }

                if (!$lockAcquired) {
                    // Could not acquire lock in the given time
                    fclose($cachedFileStream);
                    continue;
                }

                $stat = fstat($cachedFileStream);
                // Check if the file is still there since the writing operation could have
                // failed. If the file is gone, retry retrieve.
                if ($stat['nlink'] === 0) {
                    fclose($cachedFileStream);
                    continue;
                }

                // File caching may have failed and left an empty file in the cache.
                // Delete the empty file and try to cache the file again.
                if ($stat['size'] === 0) {
                    fclose($cachedFileStream);
                    $this->delete(new SplFileInfo($cachedPath));
                    continue;
                }

                // The file exists and is no longer written to.
                return $this->retrieveExistingFile($cachedPath, $cachedFileStream);
            }

            // The file did not exist and should be written. Hold LOCK_EX until writing
            // finished.
            if (!flock($cachedFileStream, LOCK_EX | LOCK_NB)) {
                // Should not happen as we just created the file, but just in case
                fclose($cachedFileStream);
                @unlink($cachedPath);
                continue;
            }

            try {
                $fileInfo = $this->retrieveNewFile($file, $cachedPath, $cachedFileStream);
                // Convert the lock so other workers can use the file from now on.
                flock($cachedFileStream, LOCK_SH);
                return $fileInfo;
            } catch (\Throwable $exception) {
                // Remove the empty file if writing failed. This is the case that is caught
                // by 'nlink' === 0 above.
                fclose($cachedFileStream);
                @unlink($cachedPath);

                throw $exception;
            }
        }

        throw FailedToRetrieveFileException::create("Failed to retrieve file after {$this->config['lock_max_attempts']} attempts");
    }

    /**
     * Get path and stream for a file that exists in the cache.
     *
     * @param string $cachedPath
     * @param resource $cachedFileStream
     *
     * @return array
     */
    protected function retrieveExistingFile(string $cachedPath, $cachedFileStream): array
    {
        // Update access and modification time to signal that this cached file was
        // used recently.
        if (!@touch($cachedPath)) {
            $this->logger->warning('Failed to update access time for cached file', [
                'path' => $cachedPath,
                'error' => error_get_last()['message'] ?? 'Unknown error',
            ]);
        }

        return [
            'path' => $cachedPath,
            'stream' => $cachedFileStream,
        ];
    }

    /**
     * Get path and stream for a file that does not yet exist in the cache.
     *
     * @param File $file
     * @param string $cachedPath
     * @param resource $cachedFileStream
     *
     * @return array
     *
     * @throws GuzzleException
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     * @throws MimeTypeIsNotAllowedException
     */
    protected function retrieveNewFile(File $file, string $cachedPath, $cachedFileStream): array
    {
        if ($this->isRemote($file)) {
            $cachedPath = $this->getRemoteFile($file, $cachedFileStream);
        } else {
            $newCachedPath = $this->getDiskFile($file, $cachedFileStream);

            // If it is a locally stored file, delete the empty "placeholder"
            // file again. The stream may stay open; it doesn't matter.
            if ($newCachedPath !== $cachedPath) {
                @unlink($cachedPath);
            }

            $cachedPath = $newCachedPath;
        }

        if (!empty($this->config['mime_types'])) {
            $type = $this->files->mimeType($cachedPath);
            if (!in_array($type, $this->config['mime_types'], true)) {
                throw MimeTypeIsNotAllowedException::create($type);
            }
        }

        return [
            'path' => $cachedPath,
            'stream' => $cachedFileStream,
        ];
    }

    /**
     * Cache a remote file and get the path to the cached file.
     *
     * @param File $file Remote file
     * @param resource $target Target file resource
     *
     * @return string
     *
     * @throws GuzzleException
     * @throws FileIsTooLargeException
     * @throws SourceResourceTimedOutException
     * @throws SourceResourceIsInvalidException
     * @throws HostNotAllowedException
     */
    protected function getRemoteFile(File $file, $target): string
    {
        $this->validateHost($file->getUrl());

        $cachedPath = $this->getCachedPath($file);

        $maxBytes = $this->config['max_file_size'];
        $isUnlimitedSize = $maxBytes < 0;

        $attempt = 0;
        $maxRetries = $this->config['http_retries'];
        $retryDelay = $this->config['http_retry_delay'];

        $lastException = null;

        while ($attempt <= $maxRetries) {
            $attempt++;

            try {
                return $this->fetchRemoteFile($file, $target, $cachedPath, $maxBytes, $isUnlimitedSize);
            } catch (RequestException $exception) {
                $lastException = $exception;
                $previous = $exception->getPrevious();
                if ($previous instanceof FileIsTooLargeException) {
                    throw $previous;
                }

                // Don't retry on client errors (4xx) except for 429 (rate limit)
                $statusCode = $exception->hasResponse() ? $exception->getResponse()->getStatusCode() : 0;
                if ($statusCode >= 400 && $statusCode < 500 && $statusCode !== 429) {
                    throw $exception;
                }

                if ($attempt <= $maxRetries) {
                    $this->logger->warning("HTTP request failed, retrying ({$attempt}/{$maxRetries})", [
                        'url' => $file->getUrl(),
                        'exception' => $exception->getMessage(),
                    ]);
                    usleep($retryDelay * 1000);
                    continue;
                }

                throw $exception;
            } catch (FailedToRetrieveFileException $exception) {
                $lastException = $exception;

                // Extract status code from message for retry decision
                if (preg_match('/status code (\d+)/', $exception->getMessage(), $matches)) {
                    $statusCode = (int) $matches[1];
                    // Don't retry on client errors (4xx) except for 429 (rate limit)
                    if ($statusCode >= 400 && $statusCode < 500 && $statusCode !== 429) {
                        throw $exception;
                    }
                }

                if ($attempt <= $maxRetries) {
                    $this->logger->warning("HTTP request failed, retrying ({$attempt}/{$maxRetries})", [
                        'url' => $file->getUrl(),
                        'exception' => $exception->getMessage(),
                    ]);
                    usleep($retryDelay * 1000);
                    continue;
                }

                throw $exception;
            }
        }

        // This is reached if maxRetries is 0 and first attempt fails without exception
        throw $lastException ?? FailedToRetrieveFileException::create('Failed to fetch remote file');
    }

    /**
     * Actually fetch the remote file content.
     *
     * @param File $file
     * @param resource $target
     * @param string $cachedPath
     * @param int $maxBytes
     * @param bool $isUnlimitedSize
     * @return string
     *
     * @throws GuzzleException
     * @throws FileIsTooLargeException
     * @throws SourceResourceTimedOutException
     * @throws SourceResourceIsInvalidException
     * @throws FailedToRetrieveFileException
     */
    protected function fetchRemoteFile(File $file, $target, string $cachedPath, int $maxBytes, bool $isUnlimitedSize): string
    {
        $sourceResource = null;

        try {
            $response = $this->client->get($this->encodeUrl($file->getUrl()), [
                'stream' => true,
                'on_headers' => function ($response) use ($maxBytes, $isUnlimitedSize) {
                    if (! $response instanceof ResponseInterface) {
                        return;
                    }
                    $contentLength = $response->getHeaderLine('content-length');
                    $contentBytes = is_numeric($contentLength) ? (int) $contentLength : null;

                    if (!$isUnlimitedSize && $contentBytes !== null && $contentBytes > $maxBytes) {
                        throw FileIsTooLargeException::create($maxBytes);
                    }
                },
            ]);

            // Check for HTTP errors (4xx, 5xx) since http_errors is disabled
            $statusCode = $response->getStatusCode();
            if ($statusCode >= 400) {
                throw FailedToRetrieveFileException::create(
                    "HTTP request failed with status code {$statusCode}"
                );
            }

            $responseBodyStream = $response->getBody();
            $sourceResource = $responseBodyStream->detach();

            if (!is_resource($sourceResource)) {
                throw SourceResourceIsInvalidException::create('Could not detach valid stream resource from response body.');
            }

            $this->copyStreamWithSizeLimit($sourceResource, $target, $maxBytes, $isUnlimitedSize, 'from remote source');

            return $cachedPath;
        } finally {
            if (is_resource($sourceResource)) {
                fclose($sourceResource);
            }
        }
    }

    /**
     * Cache a file from a storage disk and get the path to the cached file. Files
     * from local disks are not cached.
     *
     * @param File $file Cloud storage file
     * @param resource $target Target file resource
     *
     * @return string
     *
     * @throws FileNotFoundException
     * @throws FileIsTooLargeException
     * @throws SourceResourceIsInvalidException
     * @throws SourceResourceTimedOutException
     */
    protected function getDiskFile(File $file, $target): string
    {
        $parts = $this->splitByProtocol($file->getUrl());
        if (!isset($parts[1])) {
            throw new FileNotFoundException("Invalid file URL: {$file->getUrl()}");
        }

        $path = $parts[1];
        $disk = $this->getDisk($file);

        // Files from the local driver are not cached.
        $source = $disk->readStream($path);
        if (is_null($source)) {
            throw new FileNotFoundException("Could not open file stream for path: {$path}");
        }

        try {
            return $this->cacheFromResource($file, $source, $target);
        } finally {
            // Always close the source stream
            if (is_resource($source)) {
                fclose($source);
            }
        }
    }

    /**
     * Store the file from the given resource to a cached file.
     *
     * @param File $file
     * @param resource $source
     * @param resource $target
     *
     * @return string Path to the cached file
     *
     * @throws SourceResourceIsInvalidException
     * @throws FileIsTooLargeException
     * @throws SourceResourceTimedOutException
     */
    protected function cacheFromResource(File $file, $source, $target): string
    {
        if (!is_resource($source)) {
            throw SourceResourceIsInvalidException::create('The source resource could not be established.');
        }

        $maxBytes = $this->config['max_file_size'];
        $isUnlimitedSize = $maxBytes < 0;

        $this->copyStreamWithSizeLimit($source, $target, $maxBytes, $isUnlimitedSize);

        return $this->getCachedPath($file);
    }

    /**
     * Copy stream with size limit and timeout handling.
     *
     * @param resource $source
     * @param resource $target
     * @param int $maxBytes
     * @param bool $isUnlimitedSize
     * @param string $errorContext Additional context for error messages
     *
     * @throws SourceResourceIsInvalidException
     * @throws FileIsTooLargeException
     * @throws SourceResourceTimedOutException
     */
    protected function copyStreamWithSizeLimit($source, $target, int $maxBytes, bool $isUnlimitedSize, string $errorContext = ''): void
    {
        $readTimeout = $this->config['read_timeout'];
        stream_set_timeout($source, $readTimeout < 0 ? 0 : (int) $readTimeout);

        $bytes = stream_copy_to_stream($source, $target, $isUnlimitedSize ? -1 : $maxBytes + 1);

        if ($bytes === false) {
            $metadata = stream_get_meta_data($source);
            if (isset($metadata['timed_out']) && $metadata['timed_out']) {
                throw SourceResourceTimedOutException::create();
            }
            $message = 'Failed to copy stream data';
            if ($errorContext) {
                $message .= " {$errorContext}";
            }
            throw SourceResourceIsInvalidException::create($message);
        }

        if (!$isUnlimitedSize && $bytes > $maxBytes) {
            throw FileIsTooLargeException::create($maxBytes);
        }

        $metadata = stream_get_meta_data($source);
        if (isset($metadata['timed_out']) && $metadata['timed_out']) {
            throw SourceResourceTimedOutException::create();
        }
    }

    /**
     * Get the path to the cached file.
     */
    protected function getCachedPath(File $file): string
    {
        $hash = hash('sha256', $file->getUrl());

        return "{$this->config['path']}/{$hash}";
    }

    /**
     * Get the storage disk on which a file is stored.
     */
    protected function getDisk(File $file): \Illuminate\Contracts\Filesystem\Filesystem
    {
        $parts = $this->splitByProtocol($file->getUrl());
        if (!isset($parts[0])) {
            throw new RuntimeException("Invalid file URL format: {$file->getUrl()}");
        }

        $diskName = $parts[0];
        // Throws an exception if the disk does not exist.
        return $this->storage->disk($diskName);
    }

    /**
     * Prepare and validate configuration values.
     *
     * @throws InvalidConfigurationException
     */
    protected function prepareConfig(array $config): array
    {
        $fileCacheConfig = config('file-cache', []);
        $config = [
            'max_file_size' => -1, // any size (-1 = unlimited)
            'max_age' => 60, // 1 hour in minutes
            'max_size' => 1E+9, // 1 GB
            'lock_max_attempts' => 3, // 3 attempts
            'lock_wait_timeout' => -1, // indefinitely (-1 = no limit)
            'timeout' => -1, // indefinitely (-1 = no limit)
            'connect_timeout' => 30.0, // 30 seconds
            'read_timeout' => 30.0, // 30 seconds
            'prune_timeout' => 300, // 5 minutes
            'mime_types' => [],
            'allowed_hosts' => null, // null = all hosts allowed
            'http_retries' => 0, // no retries by default
            'http_retry_delay' => 100, // 100ms between retries
            'batch_chunk_size' => 100, // chunk size for batch operations
            'path' => storage_path('framework/cache/files'),
            ...(is_array($fileCacheConfig) ? $fileCacheConfig : []),
            ...$config,
        ];

        // Convert values to appropriate types
        $config['max_file_size'] = (int)$config['max_file_size'];
        $config['max_age'] = (int)$config['max_age'];
        $config['max_size'] = (int)$config['max_size'];
        $config['lock_max_attempts'] = (int)$config['lock_max_attempts'];
        $config['lock_wait_timeout'] = (float)$config['lock_wait_timeout'];
        $config['timeout'] = (float)$config['timeout'];
        $config['connect_timeout'] = (float)$config['connect_timeout'];
        $config['read_timeout'] = (float)$config['read_timeout'];
        $config['prune_timeout'] = (int)$config['prune_timeout'];
        $config['http_retries'] = max((int)$config['http_retries'], 0);
        $config['http_retry_delay'] = max((int)$config['http_retry_delay'], 0);
        $config['batch_chunk_size'] = (int)$config['batch_chunk_size'];

        // Parse allowed_hosts from env string if needed
        if (is_string($config['allowed_hosts']) && !empty($config['allowed_hosts'])) {
            $config['allowed_hosts'] = array_map('trim', explode(',', $config['allowed_hosts']));
        }

        // Validate configuration values
        $this->validateConfig($config);

        return $config;
    }

    /**
     * Validate configuration values.
     *
     * @throws InvalidConfigurationException
     */
    protected function validateConfig(array $config): void
    {
        if ($config['max_file_size'] < -1) {
            throw InvalidConfigurationException::create('max_file_size', 'must be -1 (unlimited) or a positive number');
        }

        if ($config['max_age'] < 1) {
            throw InvalidConfigurationException::create('max_age', 'must be at least 1 minute');
        }

        if ($config['max_size'] < 0) {
            throw InvalidConfigurationException::create('max_size', 'must be 0 or a positive number');
        }

        if ($config['lock_max_attempts'] < 1) {
            throw InvalidConfigurationException::create('lock_max_attempts', 'must be at least 1');
        }

        if ($config['lock_wait_timeout'] < -1) {
            throw InvalidConfigurationException::create('lock_wait_timeout', 'must be -1 (indefinitely) or a non-negative number');
        }

        if ($config['timeout'] < -1) {
            throw InvalidConfigurationException::create('timeout', 'must be -1 (indefinitely) or a non-negative number');
        }

        if ($config['connect_timeout'] < -1) {
            throw InvalidConfigurationException::create('connect_timeout', 'must be -1 (indefinitely) or a non-negative number');
        }

        if ($config['read_timeout'] < -1) {
            throw InvalidConfigurationException::create('read_timeout', 'must be -1 (indefinitely) or a non-negative number');
        }

        if ($config['prune_timeout'] < -1) {
            throw InvalidConfigurationException::create('prune_timeout', 'must be -1 (no timeout) or a non-negative number');
        }

        if ($config['batch_chunk_size'] < -1 || $config['batch_chunk_size'] === 0) {
            throw InvalidConfigurationException::create('batch_chunk_size', 'must be -1 (no limit) or a positive number');
        }
    }

    /**
     * Creates the cache directory if it doesn't exist yet.
     */
    protected function ensurePathExists(): void
    {
        if (!$this->files->exists($this->config['path'])) {
            $this->files->makeDirectory($this->config['path'], 0755, true, true);
        }
    }

    /**
     * Determine if a file is remote, i.e. served by a public webserver.
     */
    protected function isRemote(File $file): bool
    {
        $url = $file->getUrl();

        return str_starts_with($url, 'http://') || str_starts_with($url, 'https://');
    }

    /**
     * Split URL by protocol separator.
     */
    protected function splitByProtocol(string $url): array
    {
        return explode('://', $url, 2);
    }

    /**
     * Escape special characters (e.g. spaces) that may occur in parts of a HTTP URL.
     *
     * We encode spaces and other problematic characters while preserving + signs
     * since they have special meaning in URLs (especially query strings).
     */
    protected function encodeUrl(string $url): string
    {
        // Characters to encode (space and other commonly problematic chars)
        // Note: We don't encode + because it has meaning in URLs (especially query strings)
        $pattern = [' ', '[', ']', '{', '}', '"', '<', '>', '\\', '^', '`', '|'];
        $replacement = ['%20', '%5B', '%5D', '%7B', '%7D', '%22', '%3C', '%3E', '%5C', '%5E', '%60', '%7C'];

        return str_replace($pattern, $replacement, $url);
    }

    /**
     * Validate that a URL's host is in the allowed hosts list.
     *
     * @throws HostNotAllowedException
     */
    protected function validateHost(string $url): void
    {
        $allowedHosts = $this->config['allowed_hosts'];

        if ($allowedHosts === null || (is_array($allowedHosts) && empty($allowedHosts))) {
            return;
        }

        $parts = parse_url($url);
        if (!isset($parts['host'])) {
            throw HostNotAllowedException::create('(empty)');
        }

        $host = strtolower($parts['host']);

        foreach ((array) $allowedHosts as $allowedHost) {
            $allowedHost = strtolower(trim($allowedHost));

            if ($host === $allowedHost) {
                return;
            }

            if (str_starts_with($allowedHost, '*.')) {
                $domain = substr($allowedHost, 2);
                if ($host === $domain || str_ends_with($host, '.' . $domain)) {
                    return;
                }
            }
        }

        throw HostNotAllowedException::create($host);
    }

    /**
     * Default callback for get() method.
     */
    protected static function defaultGetCallback(File $file, string $path): string
    {
        return $path;
    }

    /**
     * Default callback for batch() method.
     */
    protected static function defaultBatchCallback(array $files, array $paths): array
    {
        return $paths;
    }

    /**
     * Create a new Guzzle HTTP client.
     */
    protected function makeHttpClient(): ClientInterface
    {
        // Guzzle uses 0 for no timeout, but we use -1 in config for clarity
        // Convert -1 to 0 for Guzzle compatibility
        $timeout = max($this->config['timeout'], 0);
        $connectTimeout = max($this->config['connect_timeout'], 0);
        $readTimeout = max($this->config['read_timeout'], 0);

        return new Client([
            'timeout' => $timeout,
            'connect_timeout' => $connectTimeout,
            'read_timeout' => $readTimeout,
            'http_errors' => false,
        ]);
    }
}
