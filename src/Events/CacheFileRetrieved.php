<?php

namespace Jackardios\FileCache\Events;

use Jackardios\FileCache\Contracts\File;

class CacheFileRetrieved
{
    public function __construct(
        public readonly File $file,
        public readonly string $cachedPath,
        public readonly int $bytes,
        public readonly string $source
    ) {
    }
}
