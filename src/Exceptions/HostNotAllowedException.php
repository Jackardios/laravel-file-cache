<?php

namespace Jackardios\FileCache\Exceptions;

use Exception;
use Throwable;

class HostNotAllowedException extends Exception
{
    public static function create(string $host, int $code = 0, ?Throwable $previous = null): self
    {
        return new self("Host '{$host}' is not in the allowed hosts list.", $code, $previous);
    }
}
