<?php

namespace Jackardios\FileCache\Events;

class CachePruneCompleted
{
    public function __construct(
        public readonly int $deleted,
        public readonly int $remaining,
        public readonly int $totalSize,
        public readonly bool $completed
    ) {
    }
}
