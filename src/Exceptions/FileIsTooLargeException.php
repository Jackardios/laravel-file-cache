<?php

namespace Jackardios\FileCache\Exceptions;

use Exception;
use Throwable;

class FileIsTooLargeException extends Exception
{
    public static function create(int $maxBytes, int $code = 0, ?Throwable $previous = null): self
    {
        return new self("The file is too large with more than {$maxBytes} bytes.", $code, $previous);
    }
}
