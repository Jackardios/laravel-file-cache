<?php

namespace Jackardios\FileCache\Tests;

use Jackardios\FileCache\Contracts\File;
use Jackardios\FileCache\Exceptions\FileIsTooLargeException;
use Jackardios\FileCache\Exceptions\FileLockedException;
use Jackardios\FileCache\Exceptions\MimeTypeIsNotAllowedException;
use Jackardios\FileCache\FileCache;
use Jackardios\FileCache\GenericFile;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Illuminate\Contracts\Filesystem\FileNotFoundException;
use Illuminate\Filesystem\FilesystemAdapter;
use Illuminate\Filesystem\FilesystemManager;
use Jackardios\FileCache\Exceptions\FailedToRetrieveFileException;
use Jackardios\FileCache\Exceptions\SourceResourceIsInvalidException;
use Jackardios\FileCache\Exceptions\SourceResourceTimedOutException;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use phpmock\phpunit\PHPMock;
use ReflectionMethod;

/**
 * @runTestsInSeparateProcesses
 * @preserveGlobalState disabled
 */
class FileCacheTest extends TestCase
{
    use PHPMock;

    public function setUp(): void
    {
        parent::setUp();
        $this->cachePath = sys_get_temp_dir().'/biigle_file_cache_test';
        $this->diskPath = sys_get_temp_dir().'/biigle_file_cache_disk';
        $this->noop = function ($file, $path) {
            return $path;
        };
        $this->app['files']->makeDirectory($this->cachePath, 0755, false, true);
        $this->app['files']->makeDirectory($this->diskPath, 0755, false, true);

        config(['filesystems.disks.test' => [
            'driver' => 'local',
            'root' => $this->diskPath,
        ]]);

        config(['filesystems.disks.fixtures' => [
            'driver' => 'local',
            'root' => __DIR__.'/files',
        ]]);
    }

    public function tearDown(): void
    {
        $this->app['files']->deleteDirectory($this->cachePath);
        $this->app['files']->deleteDirectory($this->diskPath);
        parent::tearDown();
    }

    public function testGetExists()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $file = new GenericFile('abc://some/image.jpg');
        $hash = hash('sha256', 'abc://some/image.jpg');

        $path = "{$this->cachePath}/{$hash}";
        copy(__DIR__.'/files/test-image.jpg', $path);
        $this->assertTrue(touch($path, time() - 1));
        $fileatime = fileatime($path);
        $this->assertNotEquals(time(), $fileatime);
        $file = $cache->get($file, function ($file, $path) {
            return $file;
        });
        $this->assertInstanceof(File::class, $file);
        clearstatcache();
        $this->assertNotEquals($fileatime, fileatime($path));
    }

    public function testGetRemote()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $mock = new MockHandler([
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
        $path = $cache->get($file, $this->noop);
        $this->assertEquals("{$this->cachePath}/{$hash}", $path);
        $this->assertTrue($this->app['files']->exists("{$this->cachePath}/{$hash}"));
    }

    public function testGetRemoteTooLarge()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $mock = new MockHandler([
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);
        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => 1,
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $this->expectException(FileIsTooLargeException::class);
        $cache->get($file, $this->noop);
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
    }

    public function testGetDiskDoesNotExist()
    {
        $file = new GenericFile('abc://files/image.jpg');
        $cache = new FileCache(['path' => $this->cachePath]);

        try {
            $cache->get($file, $this->noop);
            $this->fail('Expected an Exception to be thrown.');
        } catch (Exception $e) {
            $this->assertStringContainsString("Disk [abc] does not have a configured driver", $e->getMessage());
        }
    }

    public function testGetDiskLocal()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $file = new GenericFile('test://test-image.jpg');
        $hash = hash('sha256', 'test://test-image.jpg');
        $cache = new FileCache(['path' => $this->cachePath]);

        $path = $cache->get($file, $this->noop);
        $this->assertEquals("{$this->cachePath}/{$hash}", $path);
    }

    public function testGetDiskLocalDoesNotExist()
    {
        $file = new GenericFile('test://test-image.jpg');
        $cache = new FileCache(['path' => $this->cachePath]);

        $this->expectException(FileNotFoundException::class);
        $cache->get($file, $this->noop);
    }

    public function testGetDiskCloud()
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);
        $file = new GenericFile('s3://files/test-image.jpg');
        $hash = hash('sha256', 's3://files/test-image.jpg');

        $stream = fopen(__DIR__.'/files/test-image.jpg', 'rb');
        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemMock->method('getDriver')->willReturn($filesystemMock);
        $filesystemMock->method('get')->willReturn($filesystemMock);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;

        $cache = new FileCache(['path' => $this->cachePath]);

        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
        $path = $cache->get($file, $this->noop);
        $this->assertEquals("{$this->cachePath}/{$hash}", $path);
        $this->assertTrue($this->app['files']->exists("{$this->cachePath}/{$hash}"));
        $this->assertFalse(is_resource($stream));
    }

    public function testGetDiskCloudTooLarge()
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);
        $file = new GenericFile('s3://files/test-image.jpg');
        $hash = hash('sha256', 's3://files/test-image.jpg');

        $stream = fopen(__DIR__.'/files/test-image.jpg', 'rb');

        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemMock->method('getDriver')->willReturn($filesystemMock);
        $filesystemMock->method('get')->willReturn($filesystemMock);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => 1,
        ]);

        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
        $this->expectException(FileIsTooLargeException::class);
        $cache->get($file, $this->noop);
    }

    public function testGetThrowOnLock()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $file = new GenericFile('abc://some/image.jpg');
        $hash = hash('sha256', 'abc://some/image.jpg');
        $path = "{$this->cachePath}/{$hash}";
        touch($path, time() - 1);

        $handle = fopen($path, 'w');
        flock($handle, LOCK_EX);

        $this->expectException(FileLockedException::class);
        $cache->get($file, fn ($file, $path) => $file, true);
    }

    public function testGetIgnoreZeroSize()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $file = new GenericFile('fixtures://test-file.txt');
        $hash = hash('sha256', 'fixtures://test-file.txt');

        $path = "{$this->cachePath}/{$hash}";
        touch($path);
        $this->assertEquals(0, filesize($path));

        $file = $cache->get($file, function ($file, $path) {
            return $file;
        });

        $this->assertNotEquals(0, filesize($path));
    }

    public function testGetOnce()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        $cache = new FileCache(['path' => $this->cachePath]);
        $file = $cache->getOnce($file, function ($file, $path) {
            return $file;
        });
        $this->assertInstanceof(File::class, $file);
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
    }

    public function testBatch()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $file = new GenericFile('test://test-image.jpg');
        $file2 = new GenericFile('test://test-image.jpg');
        $hash = hash('sha256', 'test://test-image.jpg');

        $cache = new FileCache(['path' => $this->cachePath]);
        $paths = $cache->batch([$file, $file2], function ($files, $paths) {
            return $paths;
        });

        $this->assertCount(2, $paths);
        $this->assertStringContainsString($hash, $paths[0]);
        $this->assertStringContainsString($hash, $paths[1]);
    }

    public function testBatchThrowOnLock()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $file = new GenericFile('abc://some/image.jpg');
        $hash = hash('sha256', 'abc://some/image.jpg');
        $path = "{$this->cachePath}/{$hash}";
        touch($path, time() - 1);

        $handle = fopen($path, 'w');
        flock($handle, LOCK_EX);

        $this->expectException(FileLockedException::class);
        $cache->batch([$file], fn ($file, $path) => $file, true);
    }

    public function testBatchOnce()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        (new FileCache(['path' => $this->cachePath]))->batchOnce([$file], $this->noop);
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
    }

    public function testPrune()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        touch("{$this->cachePath}/abc", time() - 1);
        $this->app['files']->put("{$this->cachePath}/def", 'def');

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_size' => 3,
        ]);
        $cache->prune();
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/abc"));
        $this->assertTrue($this->app['files']->exists("{$this->cachePath}/def"));

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_size' => 0,
        ]);
        $cache->prune();
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/def"));
    }

    public function testPruneAge()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        touch("{$this->cachePath}/abc", time() - 61);
        $this->app['files']->put("{$this->cachePath}/def", 'def');

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_age' => 1,
        ]);
        $cache->prune();
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/abc"));
        $this->assertTrue($this->app['files']->exists("{$this->cachePath}/def"));
    }

    public function testClear()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        $this->app['files']->put("{$this->cachePath}/def", 'abc');
        $handle = fopen("{$this->cachePath}/def", 'rb');
        flock($handle, LOCK_SH);
        (new FileCache(['path' => $this->cachePath]))->clear();
        fclose($handle);
        $this->assertTrue($this->app['files']->exists("{$this->cachePath}/def"));
        $this->assertFalse($this->app['files']->exists("{$this->cachePath}/abc"));
    }

    public function testMimeTypeWhitelist()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'mime_types' => ['image/jpeg'],
        ]);
        $cache->get(new GenericFile('fixtures://test-image.jpg'), $this->noop);

        try {
            $cache->get(new GenericFile('fixtures://test-file.txt'), $this->noop);
            $this->fail('Expected an Exception to be thrown.');
        } catch (MimeTypeIsNotAllowedException $e) {
            $this->assertStringContainsString("text/plain", $e->getMessage());
        }
    }

    public function testExistsDisk()
    {
        $file = new GenericFile('test://test-image.jpg');
        $cache = new FileCache(['path' => $this->cachePath]);

        $this->assertFalse($cache->exists($file));
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $this->assertTrue($cache->exists($file));
    }

    public function testExistsDiskTooLarge()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $file = new GenericFile('test://test-image.jpg');
        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => 1,
        ]);

        $this->expectException(FileIsTooLargeException::class);
        $cache->exists($file);
    }

    public function testExistsDiskMimeNotAllowed()
    {
        $this->app['files']->put("{$this->diskPath}/test-file.txt", 'abc');
        $file = new GenericFile('test://test-file.txt');
        $cache = new FileCache([
            'path' => $this->cachePath,
            'mime_types' => ['image/jpeg'],
        ]);

        try {
            $cache->exists($file);
            $this->fail('Expected an Exception to be thrown.');
        } catch (MimeTypeIsNotAllowedException $e) {
            $this->assertStringContainsString("text/plain", $e->getMessage());
        }
    }

    public function testExistsRemote404()
    {
        $mock = new MockHandler([new Response(404)]);
        $handlerStack = HandlerStack::create($mock);
        $client = new Client([
            'handler' => $handlerStack,
            'http_errors' => false,
        ]);

        $file = new GenericFile('https://example.com/file');
        $cache = new FileCache(['path' => $this->cachePath], client: $client);
        $this->assertFalse($cache->exists($file));
    }

    public function testExistsRemote500()
    {
        $mock = new MockHandler([new Response(500)]);
        $handlerStack = HandlerStack::create($mock);
        $client = new Client([
            'handler' => $handlerStack,
            'http_errors' => false,
        ]);

        $file = new GenericFile('https://example.com/file');
        $cache = new FileCache(['path' => $this->cachePath], client: $client);
        $this->assertFalse($cache->exists($file));
    }

    public function testExistsRemote200()
    {
        $mock = new MockHandler([new Response(200)]);
        $handlerStack = HandlerStack::create($mock);
        $client = new Client([
            'handler' => $handlerStack,
            'http_errors' => false,
        ]);

        $file = new GenericFile('https://example.com/file');
        $cache = new FileCache(['path' => $this->cachePath], client: $client);
        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteTooLarge()
    {
        $mock = new MockHandler([new Response(200, ['content-length' => 100])]);
        $handlerStack = HandlerStack::create($mock);
        $client = new Client([
            'handler' => $handlerStack,
            'http_errors' => false,
        ]);

        $file = new GenericFile('https://example.com/file');
        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => 50,
        ], client: $client);

        $this->expectException(FileIsTooLargeException::class);
        $cache->exists($file);
    }

    public function testExistsRemoteMimeNotAllowed()
    {
        $mock = new MockHandler([new Response(200, ['content-type' => 'application/json'])]);
        $handlerStack = HandlerStack::create($mock);
        $client = new Client([
            'handler' => $handlerStack,
            'http_errors' => false,
        ]);

        $file = new GenericFile('https://example.com/file');
        $cache = new FileCache([
            'path' => $this->cachePath,
            'mime_types' => ['image/jpeg'],
        ], client: $client);

        try {
            $cache->exists($file);
            $this->fail('Expected an Exception to be thrown.');
        } catch (MimeTypeIsNotAllowedException $e) {
            $this->assertStringContainsString("application/json", $e->getMessage());
        }
    }

    public function testGetRemoteThrowsGuzzleExceptionOnGetRequest()
    {
        $file = new GenericFile('https://files.example.com/image.jpg');
        $hash = hash('sha256', 'https://files.example.com/image.jpg');
        $request = new Request('GET', $file->getUrl());
        $connectException = new ConnectException('Connection failed', $request);

        $mock = new MockHandler([$connectException]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        $this->expectException(ConnectException::class); // Ожидаем исходное исключение Guzzle

        try {
            $cache->get($file, $this->noop);
        } finally {
            // Убедимся, что временный файл не остался (если он был создан)
            $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
        }
    }

    public function testGetDiskThrowsSourceResourceTimeoutException()
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);
        $file = new GenericFile('s3://files/test-image.jpg');
        $hash = hash('sha256', 's3://files/test-image.jpg');
        $stream = fopen('php://temp', 'r+');
        fwrite($stream, 'some data');
        rewind($stream);

        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;

        $streamGetMetaDataMock = $this->getFunctionMock('Jackardios\\FileCache', 'stream_get_meta_data');
        $streamGetMetaDataMock->expects($this->atLeastOnce())->willReturn(['timed_out' => true]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'read_timeout' => 0.1,
        ]);

        $this->expectException(SourceResourceTimedOutException::class);

        try {
            $cache->get($file, $this->noop);
        } finally {
            $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
            if (is_resource($stream)) {
                fclose($stream);
            }
        }
    }

    public function testGetDiskThrowsSourceResourceInvalidExceptionOnCopyFail()
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);
        $file = new GenericFile('s3://files/test-image.jpg');
        $hash = hash('sha256', 's3://files/test-image.jpg');
        $stream = fopen('php://temp', 'r+');
        fwrite($stream, 'some data');
        rewind($stream);

        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;

        $streamCopyMock = $this->getFunctionMock('Jackardios\\FileCache', 'stream_copy_to_stream');
        $streamCopyMock->expects($this->once())->willReturn(false);

        $cache = new FileCache(['path' => $this->cachePath]);

        $this->expectException(SourceResourceIsInvalidException::class);
        $this->expectExceptionMessage('Failed to copy stream data');

        try {
            $cache->get($file, $this->noop);
        } finally {
            $this->assertFalse($this->app['files']->exists("{$this->cachePath}/{$hash}"));
            if (is_resource($stream)) {
                fclose($stream);
            }
        }
    }

    public function testRetrieveThrowsFailedToRetrieveFileExceptionAfterMaxAttempts()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $hash = hash('sha256', 'fixtures://test-file.txt');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $this->assertTrue($this->app['filesystem']->disk('fixtures')->exists('test-file.txt'));
        touch($cachedPath);
        $this->assertFileExists($cachedPath);
        $fopenMock = $this->getFunctionMock('Jackardios\\FileCache', 'fopen');
        $fopenMock->expects($this->atLeast(3)) // Expects >= 3 attempts fopen()
            ->willReturnCallback(function ($path, $mode) use ($cachedPath) {
                if ($path === $cachedPath) {
                    if ($mode === 'xb+') {
                        // This call should return false since the file exists (due to touch)
                        // Let the original fopen handle it
                        return \fopen($path, $mode);
                    }
                    if ($mode === 'rb') {
                        // Simulate a permanent failure to open an existing cache file for reading
                        return false;
                    }
                }
                // Allow normal behavior for all other fopen calls
                return \fopen($path, $mode);
            });

        $usleepMock = $this->getFunctionMock('Jackardios\\FileCache', 'usleep');
        $usleepMock->expects($this->any());

        $cache = new FileCache(['path' => $this->cachePath]);

        $this->expectException(FailedToRetrieveFileException::class);
        $this->expectExceptionMessage('Failed to retrieve file after 3 attempts');

        try {
            $cache->get($file, $this->noop);
        } catch (FailedToRetrieveFileException $e) {
            $this->assertFileExists($cachedPath);
            throw $e;
        } finally {
            if (file_exists($cachedPath)) {
                unlink($cachedPath);
            }
        }
    }

    public function testPruneSkipsLockedFile()
    {
        $unlockedFile = "{$this->cachePath}/unlocked";
        $lockedFile = "{$this->cachePath}/locked";

        $this->app['files']->put($unlockedFile, 'delete me');
        touch($unlockedFile, time() - 100);
        clearstatcache(true, $unlockedFile);

        $this->app['files']->put($lockedFile, 'keep me');
        touch($lockedFile, time() - 100);
        clearstatcache(true, $lockedFile);


        $handle = fopen($lockedFile, 'rb');
        $this->assertTrue(flock($handle, LOCK_SH));

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_age' => 1,
            'max_size' => 1000^2,
        ]);

        $cache->prune();

        $this->assertFalse($this->app['files']->exists($unlockedFile), "Unlocked file should be pruned.");
        $this->assertTrue($this->app['files']->exists($lockedFile), "Locked file should NOT be pruned.");

        flock($handle, LOCK_UN);
        fclose($handle);

        $cache->prune();
        $this->assertFalse($this->app['files']->exists($lockedFile), "Unlocked file should be pruned now.");
    }

    /**
     * @test
     * @dataProvider provideUrlsForEncoding
     */
    public function testEncodeUrl(string $inputUrl, string $expectedUrl)
    {
        $cache = new FileCache();
        $method = new ReflectionMethod(FileCache::class, 'encodeUrl');
        $method->setAccessible(true);

        $this->assertEquals($expectedUrl, $method->invoke($cache, $inputUrl));
    }

    public static function provideUrlsForEncoding(): array
    {
        return [
            'no encoding needed' => ['http://example.com/path/file.jpg', 'http://example.com/path/file.jpg'],
            'space encoding' => ['http://example.com/path with space/file name.jpg', 'http://example.com/path%20with%20space/file%20name.jpg'],
            'plus sign not encoded' => ['http://example.com/path+plus/file+name.jpg', 'http://example.com/path+plus/file+name.jpg'],
            'mixed chars' => ['http://example.com/path with space/and+plus.jpg', 'http://example.com/path%20with%20space/and+plus.jpg'],
            'query string spaces encoded' => ['http://example.com/pa th?q=a+b c', 'http://example.com/pa%20th?q=a+b%20c'],
        ];
    }

    public function testExistsRemoteReturnsFalseOnGuzzleException()
    {
        $file = new GenericFile('https://files.example.com/image.jpg');
        $request = new Request('HEAD', $file->getUrl());
        $connectException = new ConnectException('Connection failed', $request);

        $mock = new MockHandler([
            $connectException,
        ]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        $this->assertFalse($cache->exists($file));
    }

    public function testGetOnceDoesNotDeleteLockedFile()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";
        $cache = new FileCache(['path' => $this->cachePath]);

        $handle = null;
        $result = $cache->getOnce($file, function ($file, $path) use (&$handle) {
            $handle = fopen($path, 'rb');
            $this->assertTrue(flock($handle, LOCK_SH));
            return $path;
        });

        $this->assertEquals($cachedPath, $result);
        $this->assertTrue($this->app['files']->exists($cachedPath));
        $this->assertNotNull($handle);

        flock($handle, LOCK_UN);
        fclose($handle);

        $this->assertTrue($this->app['files']->delete($cachedPath));
    }
}
