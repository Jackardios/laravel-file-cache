<?php

namespace Jackardios\FileCache\Contracts;

interface FileCache
{
    /**
     * Perform a callback with the path of a cached file. This takes care of shared
     * locks on the cached file files, so it is not corrupted due to concurrent write
     * operations.
     *
     * @param \Jackardios\FileCache\Contracts\File $file
     * @param (callable(\Jackardios\FileCache\Contracts\File, string): mixed)|null $callback Gets the file object and the path to the cached file
     * file as arguments.
     * @param bool $throwOnLock Whether to throw an exception if a file is currently locked (i.e. written to). Otherwise the method will wait until the lock is released.
     *
     * @return mixed Result of the callback.
     */
    public function get(File $file, ?callable $callback = null, bool $throwOnLock = false);

    /**
     * Like `get` but deletes the cached file afterwards (if it is not used somewhere
     * else).
     *
     * @param \Jackardios\FileCache\Contracts\File $file
     * @param (callable(\Jackardios\FileCache\Contracts\File, string): mixed)|null $callback Gets the file object and the path to the cached file
     * file as arguments.
     * @param bool $throwOnLock Whether to throw an exception if a file is currently locked (i.e. written to). Otherwise the method will wait until the lock is released.
     *
     * @return mixed Result of the callback.
     */
    public function getOnce(File $file, ?callable $callback = null, bool $throwOnLock = false);

    /**
     * Perform a callback with the paths of many cached files. Use this to prevent
     * pruning of the files while they are processed.
     *
     * @param \Jackardios\FileCache\Contracts\File[] $files
     * @param (callable(\Jackardios\FileCache\Contracts\File[], string[]): mixed)|null $callback Gets the array of file objects and the array of paths
     * to the cached file files (in the same ordering) as arguments.
     * @param bool $throwOnLock Whether to throw an exception if a file is currently locked (i.e. written to). Otherwise the method will wait until the lock is released.
     *
     * @return mixed Result of the callback.
     */
    public function batch(array $files, ?callable $callback = null, bool $throwOnLock = false);

    /**
     * Like `batch` but deletes the cached files afterwards (if they are not used
     * somewhere else).
     *
     * @param \Jackardios\FileCache\Contracts\File[] $files
     * @param (callable(\Jackardios\FileCache\Contracts\File[], string[]): mixed)|null $callback Gets the array of file objects and the array of paths
     * to the cached file files (in the same ordering) as arguments.
     * @param bool $throwOnLock Whether to throw an exception if a file is currently locked (i.e. written to). Otherwise the method will wait until the lock is released.
     *
     * @return mixed Result of the callback.
     */
    public function batchOnce(array $files, ?callable $callback = null, bool $throwOnLock = false);

    /**
     * Remove cached files that are too old or exceed the maximum cache size.
     *
     * @return array{deleted: int, remaining: int, total_size: int} Statistics about pruning operation
     */
    public function prune(): array;

    /**
     * Delete all unused cached files.
     */
    public function clear(): void;

    /**
     * Check if a file exists.
     *
     * @param \Jackardios\FileCache\Contracts\File $file
     *
     * @return bool Whether the file exists or not.
     */
    public function exists(File $file): bool;
}
