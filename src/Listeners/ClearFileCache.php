<?php

namespace Jackardios\FileCache\Listeners;

use Jackardios\FileCache\Contracts\FileCache as FileCacheContract;

class ClearFileCache
{
    public function __construct(protected FileCacheContract $cache)
    {
    }

    /**
     * Handle the event.
     */
    public function handle(): void
    {
        $this->cache->clear();
    }
}
