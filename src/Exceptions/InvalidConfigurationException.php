<?php

namespace Jackardios\FileCache\Exceptions;

use Exception;
use Throwable;

class InvalidConfigurationException extends Exception
{
    public static function create(string $key, string $message, int $code = 0, ?Throwable $previous = null): self
    {
        return new self("Invalid configuration for '{$key}': {$message}", $code, $previous);
    }
}
