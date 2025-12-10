<?php

namespace Jackardios\FileCache\Tests\Facades;

use Jackardios\FileCache\Facades\FileCache as FileCacheFacade;
use Jackardios\FileCache\FileCache as BaseFileCache;
use Jackardios\FileCache\GenericFile;
use Jackardios\FileCache\Tests\TestCase;
use FileCache;

class FileCacheTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
        if (!class_exists(FileCache::class)) {
            class_alias(FileCacheFacade::class, 'FileCache');
        }
    }

    public function testFacade()
    {
        $this->assertInstanceOf(BaseFileCache::class, FileCache::getFacadeRoot());
    }

    public function testFake()
    {
        FileCache::fake();
        $file = new GenericFile('https://example.com/image.jpg');
        $path = FileCache::get($file, function ($file, $path) {
            return $path;
        });

        $this->assertFalse($this->app['files']->exists($path));
    }
}
