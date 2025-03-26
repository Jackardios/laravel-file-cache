<?php

namespace Biigle\FileCache\Exceptions;

use Exception;

class FileLockedException extends Exception
{
    public static function create(?string $message = null): self
    {
        return new self($message ?? 'File is locked.');
    }
}
