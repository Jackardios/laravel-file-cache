<?php

namespace Jackardios\FileCache\Testing;

use Illuminate\Contracts\Foundation\Application;
use Jackardios\FileCache\Contracts\File;
use Jackardios\FileCache\Contracts\FileCache as FileCacheContract;
use Illuminate\Filesystem\Filesystem;

class FileCacheFake implements FileCacheContract
{
    protected string $path;

    /**
     * Create a new fake file cache instance.
     *
     * @param Application|null $app Application instance (optional, for compatibility)
     */
    public function __construct(?Application $app = null)
    {
        $storagePath = $app?->storagePath() ?? storage_path();

        (new Filesystem)->cleanDirectory(
            $root = "{$storagePath}/framework/testing/disks/file-cache"
        );

        $this->path = $root;
    }

    /**
     * {@inheritdoc}
     */
    public function get(File $file, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? static fn(File $file, string $path): string => $path;

        return $this->batch([$file], function ($files, $paths) use ($callback) {
            return call_user_func($callback, $files[0], $paths[0]);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function getOnce(File $file, ?callable $callback = null, bool $throwOnLock = false)
    {
        return $this->get($file, $callback);
    }

    /**
     * {@inheritdoc}
     */
    public function batch(array $files, ?callable $callback = null, bool $throwOnLock = false)
    {
        $callback = $callback ?? static fn(array $files, array $paths): array => $paths;

        $paths = array_map(function ($file) {
            $hash = hash('sha256', $file->getUrl());

            return "{$this->path}/{$hash}";
        }, $files);

        return $callback($files, $paths);
    }

    /**
     * {@inheritdoc}
     */
    public function batchOnce(array $files, ?callable $callback = null, bool $throwOnLock = false)
    {
        return $this->batch($files, $callback);
    }

    /**
     * {@inheritdoc}
     */
    public function prune(): array
    {
        return ['deleted' => 0, 'remaining' => 0, 'total_size' => 0];
    }

    /**
     * {@inheritdoc}
     */
    public function clear(): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function exists(File $file): bool
    {
        return false;
    }
}
