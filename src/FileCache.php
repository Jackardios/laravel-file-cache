<?php

namespace Biigle\FileCache;

use Biigle\FileCache\Contracts\File;
use Biigle\FileCache\Contracts\FileCache as FileCacheContract;
use Biigle\FileCache\Exceptions\FailedToRetrieveFileException;
use Biigle\FileCache\Exceptions\FileIsTooLargeException;
use Biigle\FileCache\Exceptions\FileLockedException;
use Biigle\FileCache\Exceptions\MimeTypeIsNotAllowedException;
use Biigle\FileCache\Exceptions\SourceResourceIsInvalidException;
use Biigle\FileCache\Exceptions\SourceResourceTimedOutException;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Psr7\LimitStream;
use GuzzleHttp\Psr7\Utils;
use Illuminate\Contracts\Filesystem\FileNotFoundException;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Filesystem\FilesystemManager;
use RuntimeException;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

/**
 * The file cache.
 */
class FileCache implements FileCacheContract
{
    /**
     * Create an instance.
     */
    public function __construct(
        protected array $config = [],
        protected ?Client $client = null,
        protected ?Filesystem $files = null,
        protected ?FilesystemManager $storage = null
    ) {
        $this->config = $this->prepareConfig($config);
        $this->client = $client ?: $this->makeHttpClient();
        $this->files = $files ?: app('files');
        $this->storage = $storage ?: app('filesystem');
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

        $retrieved = [];
        try {
            $retrieved = array_map(function ($file) use ($throwOnLock) {
                return $this->retrieve($file, $throwOnLock);
            }, $files);

            $paths = array_map(static function ($file) {
                return $file['path'];
            }, $retrieved);

            return call_user_func($callback, $files, $paths);
        } finally {
            // Ensure all streams are closed even if an exception occurs
            foreach ($retrieved as $file) {
                if (isset($file['stream']) && is_resource($file['stream'])) {
                    fclose($file['stream']);
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

        $retrieved = [];
        try {
            $retrieved = array_map(function ($file) use ($throwOnLock) {
                return $this->retrieve($file, $throwOnLock);
            }, $files);

            $paths = array_map(static function ($file) {
                return $file['path'];
            }, $retrieved);

            $result = call_user_func($callback, $files, $paths);

            // Delete processed files
            foreach ($retrieved as $index => $file) {
                // Try to obtain an exclusive lock with a non-blocking call
                if (isset($file['stream'])
                    && is_resource($file['stream'])
                    && flock($file['stream'], LOCK_EX | LOCK_NB)) {
                    $path = $this->getCachedPath($files[$index]);
                    if ($this->files->exists($path)) {
                        $this->files->delete($path);
                    }
                }
            }

            return $result;
        } finally {
            // Ensure all streams are closed even if an exception occurs
            foreach ($retrieved as $file) {
                if (isset($file['stream']) && is_resource($file['stream'])) {
                    fclose($file['stream']);
                }
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function prune(): void
    {
        if (!$this->files->exists($this->config['path'])) {
            return;
        }

        $now = time();
        // Allowed age in seconds.
        $allowedAge = $this->config['max_age'] * 60;
        $totalSize = 0;

        $files = Finder::create()
            ->files()
            ->ignoreDotFiles(true)
            ->in($this->config['path'])
            ->getIterator();

        // Prune files by age.
        foreach ($files as $file) {
            try {
                $aTime = $file->getATime();

                if (($now - $aTime) > $allowedAge && $this->delete($file)) {
                    continue;
                }

                $totalSize += $file->getSize();
            } catch (RuntimeException $e) {
                // File might have been deleted in the meantime, skip it
                continue;
            }
        }

        $allowedSize = $this->config['max_size'];

        // Prune files by cache size.
        if ($totalSize > $allowedSize) {
            $files = Finder::create()
                ->files()
                ->ignoreDotFiles(true)
                // This will return the least recently accessed files first.
                ->sortByAccessedTime()
                ->in($this->config['path'])
                ->getIterator();

            while ($totalSize > $allowedSize && ($file = $files->current())) {
                try {
                    $fileSize = $file->getSize();
                    if ($this->delete($file)) {
                        $totalSize -= $fileSize;
                    }
                } catch (RuntimeException $e) {
                    // File might have been deleted in the meantime, skip it
                }
                $files->next();
            }
        }
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
     */
    protected function existsRemote(File $file): bool
    {
        try {
            $response = $this->client->head($this->encodeUrl($file->getUrl()), [
                'timeout' => $this->config['timeout'],
                'connect_timeout' => $this->config['connect_timeout'],
            ]);
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

            $maxBytes = (int) $this->config['max_file_size'];
            $size = (int) $response->getHeaderLine('content-length');

            if ($maxBytes >= 0 && $size > $maxBytes) {
                throw FileIsTooLargeException::create($maxBytes);
            }

            return true;
        } catch (GuzzleException $e) {
            // Consider the file as non-existent if HEAD request fails
            return false;
        }
    }

    /**
     * Check for existence of a file from a storage disk.
     *
     * @throws MimeTypeIsNotAllowedException
     * @throws FileIsTooLargeException
     */
    protected function existsDisk(File $file): bool
    {
        $urlWithoutPort = $this->splitByProtocol($file->getUrl())[1] ?? null;
        if ($urlWithoutPort === null) {
            return false;
        }

        $disk = $this->getDisk($file);
        $exists = $disk->exists($urlWithoutPort);

        if (!$exists) {
            return false;
        }

        if (!empty($this->config['mime_types'])) {
            $type = $disk->mimeType($urlWithoutPort);
            if (!in_array($type, $this->config['mime_types'], true)) {
                throw MimeTypeIsNotAllowedException::create($type);
            }
        }

        $maxBytes = (int)$this->config['max_file_size'];

        if ($maxBytes >= 0) {
            $size = $disk->size($urlWithoutPort);
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
        // Check if file still exists before attempting to delete
        if (!file_exists($file->getRealPath())) {
            return false;
        }

        $fileStream = null;
        $deleted = false;

        try {
            $fileStream = @fopen($file->getRealPath(), 'rb');
            if ($fileStream === false) {
                // Cannot open file, maybe it's already deleted
                return false;
            }

            // Only delete the file if it is not currently used. Else move on.
            // Use non-blocking to avoid deadlocks
            if (flock($fileStream, LOCK_EX | LOCK_NB)) {
                $this->files->delete($file->getRealPath());
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
        $maxAttempts = 3;  // Prevent infinite loops
        $attempt = 0;

        while ($attempt < $maxAttempts) {
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
                    throw new FileLockedException();
                }

                // Wait for any LOCK_EX that is set if the file is currently written.
                // Add timeout to prevent hanging
                $lockAcquired = false;
                $startTime = microtime(true);
                $timeoutSec = 5; // 5 second timeout

                while (!$lockAcquired && (microtime(true) - $startTime) < $timeoutSec) {
                    $lockAcquired = flock($cachedFileStream, LOCK_SH | LOCK_NB);
                    if (!$lockAcquired) {
                        usleep(50000); // 50ms
                    }
                }

                if (!$lockAcquired) {
                    // Could not acquire lock in the given time
                    fclose($cachedFileStream);
                    // If timeout, try to remove the potentially corrupted file and retry
                    @unlink($cachedPath);
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
            } catch (Exception $exception) {
                // Remove the empty file if writing failed. This is the case that is caught
                // by 'nlink' === 0 above.
                @unlink($cachedPath);
                fclose($cachedFileStream);

                throw $exception;
            }
        }

        throw new FailedToRetrieveFileException("Failed to retrieve file after {$maxAttempts} attempts");
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
        touch($cachedPath);

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
     */
    protected function getRemoteFile(File $file, $target): string
    {
        $cachedPath = $this->getCachedPath($file);

        $maxBytes = $this->config['max_file_size'];
        $isUnlimitedSize = $maxBytes === -1;
        $limitedTarget = new LimitStream(Utils::streamFor($target), $isUnlimitedSize ? -1 : $maxBytes + 1);

        $response = $this->client->get($this->encodeUrl($file->getUrl()), [
            'timeout' => $this->config['timeout'],
            'connect_timeout' => $this->config['connect_timeout'],
            'read_timeout' => $this->config['read_timeout'],
            'sink' => $limitedTarget,
        ]);

        $response->getBody()->detach();

        if (!$isUnlimitedSize && $limitedTarget->getSize() > $maxBytes) {
            throw FileIsTooLargeException::create($maxBytes);
        }

        return $cachedPath;
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

        $cachedPath = $this->getCachedPath($file);
        $maxBytes = $this->config['max_file_size'];
        $isUnlimitedSize = $maxBytes === -1;

        // Set read timeout on source stream if possible
        stream_set_timeout($source, (int)$this->config['read_timeout']);

        $bytes = stream_copy_to_stream($source, $target, $isUnlimitedSize ? -1 : $maxBytes + 1);

        if ($bytes === false) {
            throw SourceResourceIsInvalidException::create('Failed to copy stream data');
        }

        if (!$isUnlimitedSize && $bytes > $maxBytes) {
            throw FileIsTooLargeException::create($maxBytes);
        }

        $metadata = stream_get_meta_data($source);

        if (array_key_exists('timed_out', $metadata) && $metadata['timed_out']) {
            throw SourceResourceTimedOutException::create();
        }

        return $cachedPath;
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
     */
    protected function prepareConfig(array $config): array
    {
        $config = [
            'max_file_size' => -1, // any size
            'max_age' => 60, // 1 hour in minutes
            'max_size' => 1E+9, // 1 GB
            'timeout' => 0, // indefinitely
            'connect_timeout' => 30.0, // 30 seconds
            'read_timeout' => 30.0, // 30 seconds
            'mime_types' => [],
            'path' => storage_path('framework/cache/files'),
            ...config('file-cache'),
            ...$config,
        ];

        // Convert values to appropriate types
        $config['max_file_size'] = (int)$config['max_file_size'];
        $config['max_age'] = (int)$config['max_age'];
        $config['max_size'] = (int)$config['max_size'];
        $config['timeout'] = (float)$config['timeout'];
        $config['connect_timeout'] = (float)$config['connect_timeout'];
        $config['read_timeout'] = (float)$config['read_timeout'];

        return $config;
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
        return str_starts_with($file->getUrl(), 'http');
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
     * We do not use urlencode or rawurlencode because they encode some characters
     * (e.g. "+") that should not be changed in the URL.
     */
    protected function encodeUrl(string $url): string
    {
        // List of characters to substitute and their replacements at the same index.
        $pattern = [' '];
        $replacement = ['%20'];

        return str_replace($pattern, $replacement, $url);
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
        return new Client([
            'timeout' => $this->config['timeout'],
            'connect_timeout' => $this->config['connect_timeout'],
            'read_timeout' => $this->config['read_timeout'],
            'http_errors' => false,
        ]);
    }
}
