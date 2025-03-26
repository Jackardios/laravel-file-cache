<?php

namespace Jackardios\FileCache\Tests\Listeners;

use Jackardios\FileCache\Tests\TestCase;

class ClearFileCacheTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->cachePath = sys_get_temp_dir().'/biigle_file_cache_test';
        $this->app['files']->makeDirectory($this->cachePath, 0755, false, true);
    }

    public function tearDown(): void
    {
        $this->app['files']->deleteDirectory($this->cachePath);
        parent::tearDown();
    }

    public function testListen()
    {
        config(['file-cache.path' => $this->cachePath]);
        $this->app['files']->put($this->cachePath.'/1', 'abc');
        $this->app['events']->dispatch('cache:clearing');
        $this->assertFalse($this->app['files']->exists($this->cachePath.'/1'));
    }
}
