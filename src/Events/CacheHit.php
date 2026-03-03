<?php

namespace Jackardios\FileCache\Events;

use Jackardios\FileCache\Contracts\File;

class CacheHit
{
    public function __construct(
        public readonly File $file,
        public readonly string $cachedPath
    ) {
    }
}
