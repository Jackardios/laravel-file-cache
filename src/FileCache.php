<?php

namespace Jackardios\FileCache;

use GuzzleHttp\Exception\RequestException;
use Jackardios\FileCache\Contracts\File;
use Jackardios\FileCache\Contracts\FileCache as FileCacheContract;
use Jackardios\FileCache\Exceptions\FailedToRetrieveFileException;
use Jackardios\FileCache\Exceptions\FileIsTooLargeException;
use Jackardios\FileCache\Exceptions\FileLockedException;
use Jackardios\FileCache\Exceptions\HostNotAllowedException;
use Jackardios\FileCache\Exceptions\MimeTypeIsNotAllowedException;
use Jackardios\FileCache\Exceptions\SourceResourceIsInvalidException;
use Jackardios\FileCache\Exceptions\SourceResourceTimedOutException;
use Jackardios\FileCache\Support\ConfigNormalizer;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Contracts\Filesystem\FileNotFoundException;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Filesystem\FilesystemAdapter;
use Illuminate\Filesystem\FilesystemManager;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

/**
 * The file cache.
 *
 * @phpstan-type RetrievedFile array{path: string, stream: resource}
 * @phpstan-type NormalizedConfig array{
 *   max_file_size: int,
 *   max_age: int,
 *   max_size: int,
 *   lock_max_attempts: int,
 *   lock_wait_timeout: float,
 *   timeout: float,
 *   connect_timeout: float,
 *   read_timeout: float,
 *   prune_timeout: int,
 *   mime_types: array<int, string>,
 *   allowed_hosts: array<int, string>|null,
 *   http_retries: int,
 *   http_retry_delay: int,
 *   lifecycle_lock_timeout: float,
 *   batch_chunk_size: int,
 *   path: string
 * }
 */
class FileCache implements FileCacheContract
{
    /**
     * @var NormalizedConfig
     */
    protected array $config;

    /**
     * HTTP client used for remote file operations.
     */
    protected Client $client;

    /**
     * Filesystem helper.
     */
    protected Filesystem $files;

    /**
     * Filesystem manager for storage disks.
     */
    protected FilesystemManager $storage;

    /**
     * Logger instance for diagnostic logging.
     */
    protected LoggerInterface $logger;

    /**
     * Create an instance.
     *
     * @param array<string, mixed> $config
     */
    public function __construct(
        array $config = [],
        ?Client $client = null,
        ?Filesystem $files = null,
        ?FilesystemManager $storage = null,
        ?LoggerInterface $logger = null
    ) {
        $this->config = ConfigNormalizer::normalize($config);
        $this->client = $client ?? $this->makeHttpClient();
        $this->files = $files ?? $this->resolveFilesystem();
        $this->storage = $storage ?? $this->resolveFilesystemManager();
        $this->logger = $logger ?: new NullLogger();
    }

    protected function resolveFilesystem(): Filesystem
    {
        $files = app('files');
        if (!$files instanceof Filesystem) {
            throw new RuntimeException('The "files" service must resolve to Illuminate\\Filesystem\\Filesystem.');
        }

        return $files;
    }

    protected function resolveFilesystemManager(): FilesystemManager
    {
        $filesystem = app('filesystem');
        if (!$filesystem instanceof FilesystemManager) {
            throw new RuntimeException('The "filesystem" service must resolve to Illuminate\\Filesystem\\FilesystemManager.');
        }

        return $filesystem;
    }

    /**
     * {@inheritdoc}
     *
     * @throws GuzzleException
     * @throws MimeTypeIsNotAllowedException
     * @throws FileIsTooLargeException
     * @throws HostNotAllowedException
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
            return $callback($files[0], $paths[0]);
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
            return $callback($files[0], $paths[0]);
        }, $throwOnLock);
    }

    /**
     * {@inheritdoc}
     *
     * @param array<int, File> $files
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

        return $this->runBatchRetrieval($files, $callback, $throwOnLock);
    }

    /**
     * Retrieve all files for batch-like operations under a shared lifecycle lock.
     *
     * @param array<int, File> $files
     * @param callable(array<int, File>, array<int, string>): mixed $callback
     * @param array<int, string> $processedPaths
     * @return mixed
     */
    protected function runBatchRetrieval(
        array $files,
        callable $callback,
        bool $throwOnLock,
        array &$processedPaths = []
    ) {
        return $this->withLifecycleSharedLock(function () use ($files, $callback, $throwOnLock, &$processedPaths) {
            return $this->runBatchByChunkStrategy($files, $callback, $throwOnLock, $processedPaths);
        });
    }

    /**
     * Execute batch processing using either direct or chunked strategy.
     *
     * @param array<int, File> $files
     * @param callable(array<int, File>, array<int, string>): mixed $callback
     * @param array<int, string> $processedPaths
     * @return mixed
     */
    protected function runBatchByChunkStrategy(
        array $files,
        callable $callback,
        bool $throwOnLock,
        array &$processedPaths = []
    ) {
        $chunkSize = $this->config['batch_chunk_size'];

        if ($chunkSize < 0 || count($files) <= $chunkSize) {
            return $this->processBatch($files, $callback, $throwOnLock, $processedPaths);
        }

        /** @var int<1, max> $chunkSize */
        return $this->processBatchChunked($files, $callback, $throwOnLock, $chunkSize, $processedPaths);
    }

    /**
     * Process files in chunks to limit concurrently opened cached file streams.
     *
     * @param array<int, File> $files
     * @param callable(array<int, File>, array<int, string>): mixed $callback
     * @param array<int, string> $processedPaths
     * @param int<1, max> $chunkSize
     * @return mixed
     */
    protected function processBatchChunked(
        array $files,
        callable $callback,
        bool $throwOnLock,
        int $chunkSize,
        array &$processedPaths = []
    )
    {
        /** @var array<int, string> $allPaths */
        $allPaths = [];
        /** @var array<int, array<int, File>> $chunks */
        $chunks = array_chunk($files, $chunkSize, true);

        foreach ($chunks as $chunkFiles) {
            /** @var array<int, RetrievedFile> $chunkRetrieved */
            $chunkRetrieved = [];

            try {
                foreach ($chunkFiles as $index => $file) {
                    $chunkRetrieved[$index] = $this->retrieve($file, $throwOnLock);
                    $allPaths[$index] = $chunkRetrieved[$index]['path'];
                    $processedPaths[$index] = $chunkRetrieved[$index]['path'];
                }
            } finally {
                foreach ($chunkRetrieved as $retrievedFile) {
                    if (is_resource($retrievedFile['stream'])) {
                        fclose($retrievedFile['stream']);
                    }
                }
            }
        }

        /** @var array<int, string> $paths */
        $paths = [];
        foreach ($files as $index => $_file) {
            if (isset($allPaths[$index])) {
                $paths[$index] = $allPaths[$index];
            }
        }

        return $callback($files, $paths);
    }

    /**
     * Process a batch of files (internal implementation).
     *
     * @param array<int, File> $files
     * @param callable(array<int, File>, array<int, string>): mixed $callback
     * @param array<int, string> $processedPaths Filled with cached paths that were successfully retrieved
     * @return mixed
     */
    protected function processBatch(array $files, callable $callback, bool $throwOnLock, array &$processedPaths = [])
    {
        /** @var array<int, RetrievedFile> $retrieved */
        $retrieved = [];
        try {
            foreach ($files as $index => $file) {
                $retrieved[$index] = $this->retrieve($file, $throwOnLock);
                $processedPaths[$index] = $retrieved[$index]['path'];
            }

            /** @var array<int, string> $paths */
            $paths = array_map(static fn(array $file): string => $file['path'], $retrieved);

            return $callback($files, $paths);
        } finally {
            foreach ($retrieved as $file) {
                if (!is_resource($file['stream'])) {
                    continue;
                }
                fclose($file['stream']);
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
        /** @var array<int, string> $processedPaths */
        $processedPaths = [];
        $result = null;
        $capturedException = null;
        $cleanupException = null;

        try {
            $result = $this->runBatchRetrieval($files, $callback, $throwOnLock, $processedPaths);
        } catch (\Throwable $exception) {
            $capturedException = $exception;
        }

        $pathsToDelete = array_values(array_unique(array_values($processedPaths)));
        if (!empty($pathsToDelete)) {
            try {
                $this->withLifecycleExclusiveLock(function () use ($pathsToDelete) {
                    $this->deleteCachedPaths($pathsToDelete);
                });
            } catch (\Throwable $exception) {
                $cleanupException = $exception;
            }
        }

        if ($capturedException !== null) {
            if ($cleanupException !== null) {
                $this->logger->warning('Failed to clean cached files after batchOnce callback exception.', [
                    'paths_count' => count($pathsToDelete),
                    'exception' => $cleanupException->getMessage(),
                ]);
            }
            throw $capturedException;
        }

        if ($cleanupException !== null) {
            throw $cleanupException;
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     *
     * @return array{deleted: int, remaining: int, total_size: int} Statistics about pruning operation
     */
    public function prune(): array
    {
        /** @var array{deleted: int, remaining: int, total_size: int} $stats */
        $stats = $this->withLifecycleExclusiveLock(function (): array {
            $stats = ['deleted' => 0, 'remaining' => 0, 'total_size' => 0];

            if (!$this->files->exists($this->config['path'])) {
                return $stats;
            }

            $startTime = time();
            $timeout = $this->config['prune_timeout'];
            $now = time();
            $allowedAge = $this->config['max_age'] * 60;
            $allowedSize = $this->config['max_size'];

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
                    continue;
                }
            }

            usort($fileInfos, static fn($a, $b) => $a['atime'] <=> $b['atime']);

            $totalSize = 0;
            $remainingCount = 0;
            $remainingFiles = [];

            foreach ($fileInfos as $info) {
                if ($this->isPruneTimedOut($startTime, $timeout, 'age-based pruning')) {
                    return $stats;
                }

                $isExpired = ($now - $info['atime']) > $allowedAge;

                if ($isExpired && $this->delete($info['file'])) {
                    $stats['deleted']++;
                    continue;
                }

                $totalSize += $info['size'];
                $remainingCount++;
                $remainingFiles[] = $info;
            }

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
                        $remainingCount--;
                    }
                }
            }

            $stats['total_size'] = $totalSize;
            $stats['remaining'] = $remainingCount;

            return $stats;
        });

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
        $this->withLifecycleExclusiveLock(function () {
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
        });
    }

    /**
     * Execute callback while holding a shared lifecycle lock.
     *
     * The shared lock keeps prune/clear and one-time deletion from removing
     * files while they are in active use.
     *
     * @param callable $callback
     * @return mixed
     */
    protected function withLifecycleSharedLock(callable $callback)
    {
        return $this->withLifecycleLock(LOCK_SH, $callback);
    }

    /**
     * Execute callback while holding an exclusive lifecycle lock.
     *
     * @param callable $callback
     * @return mixed
     */
    protected function withLifecycleExclusiveLock(callable $callback)
    {
        return $this->withLifecycleLock(LOCK_EX, $callback);
    }

    /**
     * Execute callback while holding a lifecycle lock.
     *
     * @param int $lockType
     * @param callable $callback
     * @return mixed
     */
    protected function withLifecycleLock(int $lockType, callable $callback)
    {
        $lockStream = $this->openLifecycleLockStream();
        $lockTimeout = $this->config['lifecycle_lock_timeout'];
        $hasLockTimeout = $lockTimeout >= 0;
        $startTime = microtime(true);

        while (!flock($lockStream, $lockType | LOCK_NB)) {
            if ($hasLockTimeout && (microtime(true) - $startTime) >= $lockTimeout) {
                fclose($lockStream);
                throw new RuntimeException(
                    "Failed to acquire file cache lifecycle lock within {$lockTimeout} seconds."
                );
            }

            usleep(50000); // 50ms
        }

        try {
            return $callback();
        } finally {
            flock($lockStream, LOCK_UN);
            fclose($lockStream);
        }
    }

    /**
     * Open the lifecycle lock stream.
     *
     * @return resource
     */
    protected function openLifecycleLockStream()
    {
        $path = $this->getLifecycleLockPath();
        $directory = dirname($path);

        if (!$this->files->exists($directory)) {
            $this->files->makeDirectory($directory, 0755, true, true);
        }

        $stream = @fopen($path, 'c+');
        if ($stream === false) {
            throw new RuntimeException("Failed to open file cache lifecycle lock at '{$path}'.");
        }

        return $stream;
    }

    /**
     * Get path for the lifecycle lock file.
     */
    protected function getLifecycleLockPath(): string
    {
        $suffix = hash('sha256', $this->normalizePathForLock($this->config['path']));

        return sys_get_temp_dir() . '/laravel-file-cache/locks/' . $suffix . '.lock';
    }

    /**
     * Normalize cache path before deriving lifecycle lock key.
     */
    protected function normalizePathForLock(string $path): string
    {
        $realPath = @realpath($path);
        if ($realPath !== false) {
            return $realPath;
        }

        $normalized = rtrim(str_replace('\\', '/', $path), '/');
        if ($normalized === '') {
            return '/';
        }

        if (preg_match('/^[A-Za-z]:$/', $normalized) === 1) {
            return $normalized . '/';
        }

        return $normalized;
    }

    /**
     * Delete cached paths if no active lock exists on those files.
     *
     * @param string[] $paths
     */
    protected function deleteCachedPaths(array $paths): void
    {
        foreach ($paths as $path) {
            $handle = @fopen($path, 'rb');
            if ($handle === false) {
                continue;
            }

            try {
                if (flock($handle, LOCK_EX | LOCK_NB)) {
                    $this->files->delete($path);
                }
            } finally {
                fclose($handle);
            }
        }
    }

    /**
     * Determine whether an HTTP status is retryable.
     */
    protected function shouldRetryHttpStatus(int $statusCode): bool
    {
        if ($statusCode === 0 || $statusCode === 429) {
            return true;
        }

        return $statusCode >= 500;
    }

    /**
     * Extract HTTP status code from a Guzzle exception.
     */
    protected function extractHttpStatusCode(GuzzleException $exception): int
    {
        if ($exception instanceof RequestException && $exception->hasResponse()) {
            $response = $exception->getResponse();
            if ($response !== null) {
                return $response->getStatusCode();
            }
        }

        return 0;
    }

    /**
     * Determine whether a failed HTTP request should be retried.
     */
    protected function shouldRetryHttpFailure(int $attempt, int $maxRetries, int $statusCode): bool
    {
        return $attempt <= $maxRetries && $this->shouldRetryHttpStatus($statusCode);
    }

    /**
     * Log and delay before next HTTP retry attempt.
     *
     * @param array<string, mixed> $context
     */
    protected function backoffHttpRetry(
        File $file,
        string $method,
        int $attempt,
        int $maxRetries,
        int $retryDelay,
        array $context = []
    ): void {
        $this->logger->warning("HTTP {$method} request failed, retrying ({$attempt}/{$maxRetries})", [
            'url' => $file->getUrl(),
            ...$context,
        ]);

        usleep($retryDelay * 1000);
    }

    /**
     * Extract HTTP status code from a retrieval exception message.
     */
    protected function extractRetrieveFailureStatusCode(FailedToRetrieveFileException $exception): int
    {
        if (preg_match('/status code (\d+)/', $exception->getMessage(), $matches) === 1) {
            return (int) $matches[1];
        }

        return 0;
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
        $attempt = 0;
        $maxRetries = $this->config['http_retries'];
        $retryDelay = $this->config['http_retry_delay'];

        while ($attempt <= $maxRetries) {
            $attempt++;

            try {
                $response = $this->client->head($this->encodeUrl($file->getUrl()));
                $code = $response->getStatusCode();

                if ($code < 200 || $code >= 300) {
                    if ($this->shouldRetryHttpFailure($attempt, $maxRetries, $code)) {
                        $this->backoffHttpRetry($file, 'HEAD', $attempt, $maxRetries, $retryDelay, [
                            'status_code' => $code,
                        ]);
                        continue;
                    }
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
            } catch (GuzzleException $exception) {
                $statusCode = $this->extractHttpStatusCode($exception);

                // Respect bool semantics for exists() when the client is configured
                // with http_errors=true and Guzzle throws for HTTP 4xx/5xx responses.
                if ($statusCode >= 400) {
                    if ($this->shouldRetryHttpFailure($attempt, $maxRetries, $statusCode)) {
                        $this->backoffHttpRetry($file, 'HEAD', $attempt, $maxRetries, $retryDelay, [
                            'status_code' => $statusCode,
                            'exception' => $exception->getMessage(),
                        ]);
                        continue;
                    }

                    return false;
                }

                if (!$this->shouldRetryHttpFailure($attempt, $maxRetries, $statusCode)) {
                    throw $exception;
                }

                $this->backoffHttpRetry($file, 'HEAD', $attempt, $maxRetries, $retryDelay, [
                    'exception' => $exception->getMessage(),
                ]);
            }
        }

        return false;
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
            if (!is_string($type)) {
                $type = '(unknown)';
            }
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
                return !file_exists($filePath);
            }

            if (flock($fileStream, LOCK_EX | LOCK_NB)) {
                $this->files->delete($filePath);
                $deleted = true;
            }
        } catch (Exception $e) {
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
     * @return RetrievedFile Containing the 'path' to the file and the file 'stream'. Close the stream when finished.
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

            $cachedFileStream = @fopen($cachedPath, 'xb+');

            if (is_resource($cachedFileStream)) {
                $newlyRetrieved = $this->retrieveByCreatingCacheFile($file, $cachedPath, $cachedFileStream);
                if ($newlyRetrieved !== null) {
                    return $newlyRetrieved;
                }
                continue;
            }

            $existingRetrieved = $this->retrieveFromExistingCache($cachedPath, $throwOnLock);
            if ($existingRetrieved !== null) {
                return $existingRetrieved;
            }
        }

        throw FailedToRetrieveFileException::create("Failed to retrieve file after {$this->config['lock_max_attempts']} attempts");
    }

    /**
     * Read and validate a file that already exists in cache.
     *
     * @return RetrievedFile|null
     */
    protected function retrieveFromExistingCache(string $cachedPath, bool $throwOnLock): ?array
    {
        $cachedFileStream = @fopen($cachedPath, 'rb');

        if ($cachedFileStream === false) {
            usleep(100000); // 100ms
            return null;
        }

        $closeStream = true;

        try {
            if (!$this->acquireSharedReadLock($cachedFileStream, $throwOnLock)) {
                return null;
            }

            $stat = fstat($cachedFileStream);
            if (!is_array($stat)) {
                return null;
            }

            if ($stat['nlink'] === 0) {
                return null;
            }

            if ($stat['size'] === 0) {
                fclose($cachedFileStream);
                $closeStream = false;
                $this->delete(new SplFileInfo($cachedPath));
                return null;
            }

            $closeStream = false;
            return $this->retrieveExistingFile($cachedPath, $cachedFileStream);
        } finally {
            if ($closeStream && is_resource($cachedFileStream)) {
                fclose($cachedFileStream);
            }
        }
    }

    /**
     * Acquire a shared lock on cached file stream.
     *
     * @param resource $cachedFileStream
     *
     * @throws FileLockedException
     */
    protected function acquireSharedReadLock($cachedFileStream, bool $throwOnLock): bool
    {
        if ($throwOnLock && !flock($cachedFileStream, LOCK_SH | LOCK_NB)) {
            throw FileLockedException::create();
        }

        $lockAcquired = false;
        $startTime = microtime(true);
        $lockTimeout = $this->config['lock_wait_timeout'];
        $hasLockTimeout = $lockTimeout >= 0;

        while (!$lockAcquired) {
            $lockAcquired = flock($cachedFileStream, LOCK_SH | LOCK_NB);

            if ($hasLockTimeout && (microtime(true) - $startTime) >= $lockTimeout) {
                return false;
            }

            if (!$lockAcquired) {
                usleep(50000); // 50ms
            }
        }

        return true;
    }

    /**
     * Retrieve a file by creating a new cache entry.
     *
     * @param resource $cachedFileStream
     *
     * @return RetrievedFile|null
     */
    protected function retrieveByCreatingCacheFile(File $file, string $cachedPath, $cachedFileStream): ?array
    {
        if (!flock($cachedFileStream, LOCK_EX | LOCK_NB)) {
            fclose($cachedFileStream);
            @unlink($cachedPath);
            return null;
        }

        try {
            $fileInfo = $this->retrieveNewFile($file, $cachedPath, $cachedFileStream);
            flock($cachedFileStream, LOCK_SH);
            return $fileInfo;
        } catch (\Throwable $exception) {
            fclose($cachedFileStream);
            @unlink($cachedPath);

            throw $exception;
        }
    }

    /**
     * Get path and stream for a file that exists in the cache.
     *
     * @param string $cachedPath
     * @param resource $cachedFileStream
     *
     * @return RetrievedFile
     */
    protected function retrieveExistingFile(string $cachedPath, $cachedFileStream): array
    {
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
     * @return RetrievedFile
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

            if ($newCachedPath !== $cachedPath) {
                @unlink($cachedPath);
            }

            $cachedPath = $newCachedPath;
        }

        if (!empty($this->config['mime_types'])) {
            $type = $this->files->mimeType($cachedPath);
            if (!is_string($type)) {
                $type = '(unknown)';
            }
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
            } catch (GuzzleException $exception) {
                $lastException = $exception;
                $previous = $exception->getPrevious();
                if ($previous instanceof FileIsTooLargeException) {
                    throw $previous;
                }

                $statusCode = $this->extractHttpStatusCode($exception);
                if (!$this->shouldRetryHttpFailure($attempt, $maxRetries, $statusCode)) {
                    throw $exception;
                }

                $context = ['exception' => $exception->getMessage()];
                if ($statusCode > 0) {
                    $context['status_code'] = $statusCode;
                }
                $this->backoffHttpRetry($file, 'GET', $attempt, $maxRetries, $retryDelay, $context);
                continue;
            } catch (FailedToRetrieveFileException $exception) {
                $lastException = $exception;
                $statusCode = $this->extractRetrieveFailureStatusCode($exception);

                if (!$this->shouldRetryHttpFailure($attempt, $maxRetries, $statusCode)) {
                    throw $exception;
                }

                $context = ['exception' => $exception->getMessage()];
                if ($statusCode > 0) {
                    $context['status_code'] = $statusCode;
                }
                $this->backoffHttpRetry($file, 'GET', $attempt, $maxRetries, $retryDelay, $context);
                continue;
            }
        }

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

        $source = $disk->readStream($path);
        if (is_null($source)) {
            throw new FileNotFoundException("Could not open file stream for path: {$path}");
        }

        try {
            return $this->cacheFromResource($file, $source, $target);
        } finally {
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
        if ($readTimeout >= 0) {
            $seconds = (int) floor($readTimeout);
            $microseconds = (int) round(($readTimeout - $seconds) * 1_000_000);

            if ($microseconds === 1_000_000) {
                $seconds++;
                $microseconds = 0;
            }

            stream_set_timeout($source, $seconds, $microseconds);
        }

        $bytes = stream_copy_to_stream($source, $target, $isUnlimitedSize ? -1 : $maxBytes + 1);

        if ($bytes === false) {
            /** @var array<string, mixed> $metadata */
            $metadata = stream_get_meta_data($source);
            if (($metadata['timed_out'] ?? false) === true) {
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

        /** @var array<string, mixed> $metadata */
        $metadata = stream_get_meta_data($source);
        if (($metadata['timed_out'] ?? false) === true) {
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
    protected function getDisk(File $file): FilesystemAdapter
    {
        $parts = $this->splitByProtocol($file->getUrl());
        $diskName = $parts[0];
        /** @var FilesystemAdapter $disk */
        $disk = $this->storage->disk($diskName);

        return $disk;
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
        $scheme = parse_url($file->getUrl(), PHP_URL_SCHEME);

        if (!is_string($scheme)) {
            return false;
        }

        $normalized = strtolower($scheme);

        return $normalized === 'http' || $normalized === 'https';
    }

    /**
     * Split URL by protocol separator.
     *
     * @return array{0: string, 1?: string}
     */
    protected function splitByProtocol(string $url): array
    {
        $parts = explode('://', $url, 2);

        if (isset($parts[1])) {
            return [$parts[0], $parts[1]];
        }

        return [$parts[0]];
    }

    /**
     * Escape special characters (e.g. spaces) that may occur in parts of a HTTP URL.
     *
     * We encode spaces and other problematic characters while preserving + signs
     * since they have special meaning in URLs (especially query strings).
     */
    protected function encodeUrl(string $url): string
    {
        $parts = parse_url($url);
        if ($parts === false || !isset($parts['scheme'], $parts['host'])) {
            return $this->encodeUrlUnsafeCharacters($url);
        }

        $encoded = strtolower($parts['scheme']) . '://';

        if (isset($parts['user'])) {
            $encoded .= $this->encodeUrlUnsafeCharacters($parts['user']);
            if (isset($parts['pass'])) {
                $encoded .= ':' . $this->encodeUrlUnsafeCharacters($parts['pass']);
            }
            $encoded .= '@';
        }

        $host = $parts['host'];
        if (str_contains($host, ':') && !str_starts_with($host, '[')) {
            $host = '[' . $host . ']';
        }
        $encoded .= $host;

        if (isset($parts['port'])) {
            $encoded .= ':' . $parts['port'];
        }

        $encoded .= $this->encodeUrlUnsafeCharacters($parts['path'] ?? '');

        if (isset($parts['query'])) {
            $encoded .= '?' . $this->encodeUrlUnsafeCharacters($parts['query']);
        }

        if (isset($parts['fragment'])) {
            $encoded .= '#' . $this->encodeUrlUnsafeCharacters($parts['fragment']);
        }

        return $encoded;
    }

    /**
     * Encode unsafe URL characters in a single URL component.
     */
    protected function encodeUrlUnsafeCharacters(string $value): string
    {
        $pattern = [' ', '[', ']', '{', '}', '"', '<', '>', '\\', '^', '`', '|'];
        $replacement = ['%20', '%5B', '%5D', '%7B', '%7D', '%22', '%3C', '%3E', '%5C', '%5E', '%60', '%7C'];

        return str_replace($pattern, $replacement, $value);
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
     *
     * @param array<int, File> $files
     * @param array<int, string> $paths
     * @return array<int, string>
     */
    protected static function defaultBatchCallback(array $files, array $paths): array
    {
        return $paths;
    }

    /**
     * Create a new Guzzle HTTP client.
     */
    protected function makeHttpClient(): Client
    {
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
