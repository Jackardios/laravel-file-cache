<?php

namespace Jackardios\FileCache\Events;

class CacheFileEvicted
{
    public function __construct(
        public readonly string $path,
        public readonly string $reason
    ) {
    }
}
