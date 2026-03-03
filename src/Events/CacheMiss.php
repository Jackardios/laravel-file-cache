<?php

namespace Jackardios\FileCache\Events;

use Jackardios\FileCache\Contracts\File;

class CacheMiss
{
    public function __construct(
        public readonly File $file,
        public readonly string $url
    ) {
    }
}
