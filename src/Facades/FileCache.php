<?php

namespace Jackardios\FileCache\Facades;

use Jackardios\FileCache\Testing\FileCacheFake;
use Illuminate\Support\Facades\Facade;

/**
 * @method static bool exists(\Jackardios\FileCache\Contracts\File $file)
 * @method static mixed get(\Jackardios\FileCache\Contracts\File $file, ?callable $callback = null, bool $throwOnLock = false)
 * @method static mixed getOnce(\Jackardios\FileCache\Contracts\File $file, ?callable $callback = null, bool $throwOnLock = false)
 * @method static mixed batch(\Jackardios\FileCache\Contracts\File[] $files, ?callable $callback = null, bool $throwOnLock = false)
 * @method static mixed batchOnce(\Jackardios\FileCache\Contracts\File[] $files, ?callable $callback = null, bool $throwOnLock = false)
 * @method static array prune()
 * @method static void clear()
 *
 * @see \Jackardios\FileCache\FileCache;
 */
class FileCache extends Facade
{
    /**
     * Use testing instance.
     */
    public static function fake(): void
    {
        static::swap(new FileCacheFake(static::getFacadeApplication()));
    }

    /**
     * Get the registered name of the component.
     */
    protected static function getFacadeAccessor(): string
    {
        return 'file-cache';
    }
}
