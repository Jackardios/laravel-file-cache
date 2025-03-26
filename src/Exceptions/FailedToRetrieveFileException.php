<?php

namespace Jackardios\FileCache\Exceptions;

use Exception;

class FailedToRetrieveFileException extends Exception
{
    public static function create(?string $message = null): self
    {
        return new self($message ?? 'Failed to retrieve file.');
    }
}
