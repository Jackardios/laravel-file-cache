<?php

namespace Jackardios\FileCache\Tests;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\RejectedPromise;
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
use Psr\Http\Message\RequestInterface;
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
        $cachedPath = "{$this->cachePath}/{$hash}";

        copy(__DIR__.'/files/test-image.jpg', $cachedPath);
        $this->assertTrue(touch($cachedPath, time() - 1));
        $fileatime = fileatime($cachedPath);
        $this->assertNotEquals(time(), $fileatime);
        $file = $cache->get($file, function ($file, $path) {
            return $file;
        });
        $this->assertInstanceof(File::class, $file);
        clearstatcache();
        $this->assertNotEquals($fileatime, fileatime($cachedPath));
    }

    public function testGetRemote()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $mock = new MockHandler([
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        $this->assertFileDoesNotExist($cachedPath);
        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);

        $this->assertFileExists($cachedPath);
    }

    public function testGetRemoteTooLarge()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $mock = new MockHandler([
            new Response(200, ['Content-Length' => 100], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);
        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => 1,
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $this->expectException(FileIsTooLargeException::class);
        $cache->get($file, $this->noop);
        $this->assertFileDoesNotExist($cachedPath);
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
        $cachedPath = "{$this->cachePath}/{$hash}";
        $cache = new FileCache(['path' => $this->cachePath]);

        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
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
        $cachedPath = "{$this->cachePath}/{$hash}";

        $stream = fopen(__DIR__.'/files/test-image.jpg', 'rb');
        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemMock->method('getDriver')->willReturn($filesystemMock);
        $filesystemMock->method('get')->willReturn($filesystemMock);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;

        $cache = new FileCache(['path' => $this->cachePath]);

        $this->assertFileDoesNotExist($cachedPath);
        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
        $this->assertFalse(is_resource($stream));
    }

    public function testGetDiskCloudTooLarge()
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);
        $file = new GenericFile('s3://files/test-image.jpg');
        $hash = hash('sha256', 's3://files/test-image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

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

        $this->expectException(FileIsTooLargeException::class);
        $cache->get($file, $this->noop);
        $this->assertFileDoesNotExist($cachedPath);
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

        $cachedPath = "{$this->cachePath}/{$hash}";
        touch($cachedPath);
        $this->assertEquals(0, filesize($cachedPath));

         $cache->get($file, function ($file, $path) {
            return $file;
        });

        $this->assertNotEquals(0, filesize($cachedPath));
    }

    public function testGetOnce()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $cache = new FileCache(['path' => $this->cachePath]);
        $file = $cache->getOnce($file, function ($file, $path) {
            return $file;
        });
        $this->assertInstanceof(File::class, $file);
        $this->assertFileDoesNotExist($cachedPath);
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
            'max_file_size' => 1,
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

    public function testExistsRemoteTimeout()
    {
        $file = new GenericFile('https://files.example.com/image.jpg');
        $request = new Request('HEAD', $file->getUrl());
        $connectException = new ConnectException('Example of connection failed', $request);

        $mock = new MockHandler([
            $connectException,
        ]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        try {
            $cache->exists($file);
            $this->fail('Expected an ConnectException to be thrown.');
        } catch (ConnectException $e) {
            $this->assertEquals('Example of connection failed', $e->getMessage());
        }
    }

    public function testGetRemoteThrowsConnectExceptionOnGetRequest()
    {
        $file = new GenericFile('https://files.example.com/image.jpg');
        $hash = hash('sha256', 'https://files.example.com/image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";
        $request = new Request('GET', $file->getUrl());
        $connectException = new ConnectException('Example of connection failed', $request);

        $mock = new MockHandler([$connectException]);
        $cache = new FileCache(['path' => $this->cachePath], new Client(['handler' => HandlerStack::create($mock)]));

        try {
            $cache->get($file, $this->noop);
            $this->fail('Expected an ConnectException to be thrown.');
        } catch (ConnectException $e) {
            $this->assertEquals('Example of connection failed', $e->getMessage());
        } finally {
            $this->assertFileDoesNotExist($cachedPath);
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
        $this->assertFileExists($cachedPath);
        $this->assertNotNull($handle);

        flock($handle, LOCK_UN);
        fclose($handle);

        $this->assertTrue($this->app['files']->delete($cachedPath));
    }

    public function testGetWaitsForLockReleaseWhenNotThrowing()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $hash = hash('sha256', 'fixtures://test-file.txt');
        $cachedPath = "{$this->cachePath}/{$hash}";

        // Simulate: Another process started recording and set LOCK_EX
        // Create a file manually to simulate the start of recording
        $this->assertTrue(touch($cachedPath), "Failed to create cache file for locking.");
        $writingProcessHandle = fopen($cachedPath, 'rb+');
        $this->assertIsResource($writingProcessHandle, "Failed to open handle for writing process simulation.");
        $this->assertTrue(flock($writingProcessHandle, LOCK_EX), "Failed to acquire LOCK_EX for writing simulation.");

        // Set up a mock for flock in the class under test:
        // - The first few attempts to get LOCK_SH | LOCK_NB should return false.
        // - Then one attempt should return true (simulating the removal of LOCK_EX).
        $flockMock = $this->getFunctionMock('Jackardios\\FileCache', 'flock');
        $lockAttempt = 0;
        $maxAttemptsBeforeSuccess = 3;
        $sharedLockHandle = null;

        $flockMock->expects($this->atLeast($maxAttemptsBeforeSuccess + 1))
            ->willReturnCallback(
                function ($handle, $operation) use (&$lockAttempt, $maxAttemptsBeforeSuccess, &$writingProcessHandle, $cachedPath, &$sharedLockHandle) {
                    $meta = stream_get_meta_data($handle);
                    if ($meta['uri'] === $cachedPath && $meta['mode'] === 'rb') {
                        $sharedLockHandle = $handle;
                    }

                    // Only interested in trying to get LOCK_SH without waiting
                    if ($handle === $sharedLockHandle && ($operation === (LOCK_SH | LOCK_NB) || $operation === LOCK_SH)) {
                        $lockAttempt++;
                        if ($lockAttempt <= $maxAttemptsBeforeSuccess) {
                            // Simulate that the file is still exclusively locked
                            return false;
                        } else {
                            // Simulate that exclusive lock is released
                            // Release real lock so that subsequent fstat etc. work
                            if (is_resource($writingProcessHandle)) {
                                \flock($writingProcessHandle, LOCK_UN);
                                fclose($writingProcessHandle);
                                $writingProcessHandle = null;
                            }

                            return \flock($handle, $operation); // Use real flock to set LOCK_SH
                        }
                    }

                    // For all other flock calls (e.g. initial LOCK_EX, LOCK_UN) - actual behavior
                    return \flock($handle, $operation);
                }
            );

        // Mock usleep to make sure the wait happens
        $usleepMock = $this->getFunctionMock('Jackardios\\FileCache', 'usleep');
        $usleepMock->expects($this->exactly($maxAttemptsBeforeSuccess));

        $cache = new FileCache(['path' => $this->cachePath]);
        $resultPath = $cache->get($file, $this->noop, false); // $throwOnLock = false

        $this->assertEquals($cachedPath, $resultPath);
        $this->assertFileExists($cachedPath);
        $this->assertGreaterThanOrEqual($maxAttemptsBeforeSuccess, $lockAttempt, "LOCK_SH should have been attempted multiple times.");
        $this->assertGreaterThan(0, filesize($cachedPath), "Cached file should not be empty after successful retrieval.");

        // Cleanup (if $writingProcessHandle was not closed in mock)
        if (is_resource($writingProcessHandle)) {
            \flock($writingProcessHandle, LOCK_UN);
            fclose($writingProcessHandle);
        }
    }

    public function testMultipleReadersAccessCachedFileSimultaneously()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $hash = hash('sha256', 'fixtures://test-file.txt');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $cache = new FileCache(['path' => $this->cachePath]);
        $initialPath = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $initialPath);
        $this->assertFileExists($cachedPath);

        // Simulate the first reader holding LOCK_SH
        $reader1Handle = fopen($cachedPath, 'rb');
        $this->assertTrue(flock($reader1Handle, LOCK_SH), "Reader 1 failed to acquire LOCK_SH.");

        // The second reader tries to get the file (must successfully get LOCK_SH)
        $reader2Path = null;
        $reader2Handle = null;
        try {
            $reader2Path = $cache->get($file, function($file, $path) use (&$reader2Handle) {
                // Let's try to open and block inside a callback to make sure it's possible
                $reader2Handle = fopen($path, 'rb');
                $this->assertTrue(flock($reader2Handle, LOCK_SH), "Reader 2 failed to acquire LOCK_SH while Reader 1 held lock.");
                return $path;
            });
        } catch (\Exception $e) {
            // Release the first reader's lock in case of error
            flock($reader1Handle, LOCK_UN);
            fclose($reader1Handle);
            if (is_resource($reader2Handle)) fclose($reader2Handle);
            $this->fail("Reader 2 failed to get cached file while Reader 1 held LOCK_SH: " . $e->getMessage());
        }

        $this->assertEquals($cachedPath, $reader2Path);
        $this->assertIsResource($reader2Handle, "Reader 2 handle should be a resource.");

        flock($reader1Handle, LOCK_UN);
        fclose($reader1Handle);
        flock($reader2Handle, LOCK_UN);
        fclose($reader2Handle);
    }

    public function testGetRemotePartialDownloadTimeout()
    {
        $fileUrl = 'https://files.example.com/large-file.zip';
        $file = new GenericFile($fileUrl);
        $hash = hash('sha256', $fileUrl);
        $cachedPath = "{$this->cachePath}/{$hash}";
        $partialContent = 'some partial data';
        $readTimeout = 0.1;

        $mockResponse = new Response(200, ['Content-Type' => 'application/zip']);
        $mock = new MockHandler([$mockResponse]);
        $client = new Client(['handler' => HandlerStack::create($mock)]);

        $streamCopyMock = $this->getFunctionMock('Jackardios\\FileCache', 'stream_copy_to_stream');
        $streamCopyMock->expects($this->once())
        ->willReturnCallback(function ($source, $dest, $maxLength) use ($partialContent) {
            fwrite($dest, $partialContent);
            // Simulate that copying was interrupted (returned false)
            return false;
        });

        // 3. Мок stream_get_meta_data
        // Must be called AFTER stream_copy_to_stream fails
        $streamMetaMock = $this->getFunctionMock('Jackardios\\FileCache', 'stream_get_meta_data');
        // Can be called multiple times (in cacheFromResource and in getRemoteFile)
        $streamMetaMock->expects($this->atLeastOnce())
            ->willReturn(['timed_out' => true]); // simulate timeout

        $cache = new FileCache([
            'path' => $this->cachePath,
            'read_timeout' => $readTimeout,
        ], $client);

        $this->expectException(SourceResourceTimedOutException::class);

        try {
            $cache->get($file, $this->noop);
        } catch (SourceResourceTimedOutException $e) {
            clearstatcache(true, $cachedPath);
            $this->assertFileDoesNotExist($cachedPath, "Partially downloaded file should have been deleted after timeout.");
            throw $e;
        } finally {
            if (file_exists($cachedPath)) {
                unlink($cachedPath);
            }
        }
    }
}
