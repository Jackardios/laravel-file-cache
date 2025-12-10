<?php

namespace Jackardios\FileCache\Exceptions;

use Exception;
use Throwable;

class MimeTypeIsNotAllowedException extends Exception
{
    public static function create(string $mimeType, int $code = 0, ?Throwable $previous = null): self
    {
        return new self("MIME type '{$mimeType}' not allowed.", $code, $previous);
    }
}
