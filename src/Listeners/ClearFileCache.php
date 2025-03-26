<?php

namespace Jackardios\FileCache\Listeners;

class ClearFileCache
{
    /**
     * Handle the event.
     */
    public function handle(): void
    {
        app('file-cache')->clear();
    }
}
