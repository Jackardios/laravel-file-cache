<?php

namespace Jackardios\FileCache\Tests;

use Jackardios\FileCache\Contracts\File;
use Jackardios\FileCache\Exceptions\FileIsTooLargeException;
use Jackardios\FileCache\Exceptions\FileLockedException;
use Jackardios\FileCache\Exceptions\InvalidConfigurationException;
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
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionMethod;

class FileCacheTest extends TestCase
{
    protected string $cachePath;
    protected string $diskPath;
    protected \Closure $noop;

    public function setUp(): void
    {
        parent::setUp();
        $suffix = uniqid('', true);
        $this->cachePath = sys_get_temp_dir().'/biigle_file_cache_test_'.$suffix;
        $this->diskPath = sys_get_temp_dir().'/biigle_file_cache_disk_'.$suffix;
        $this->noop = fn ($file, $path) => $path;

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
        if ($this->app['files']->exists($this->cachePath)) {
            $this->app['files']->deleteDirectory($this->cachePath);
        }
        if ($this->app['files']->exists($this->diskPath)) {
            $this->app['files']->deleteDirectory($this->diskPath);
        }
        parent::tearDown();
    }

    /**
     * Create a FileCache instance with default path.
     */
    protected function createCache(array $config = []): FileCache
    {
        return new FileCache(array_merge(['path' => $this->cachePath], $config));
    }

    /**
     * Create a FileCache with a mock HTTP client.
     */
    protected function createCacheWithMockClient(array $responses, array $config = [], bool $httpErrors = false): FileCache
    {
        $mock = new MockHandler($responses);
        $client = new Client([
            'handler' => HandlerStack::create($mock),
            'http_errors' => $httpErrors,
        ]);
        return new FileCache(array_merge(['path' => $this->cachePath], $config), $client);
    }

    /**
     * Get the cached path for a file URL.
     */
    protected function getCachedPath(string $url): string
    {
        return "{$this->cachePath}/" . hash('sha256', $url);
    }

    /**
     * Create a mock S3 filesystem.
     */
    protected function mockS3Filesystem($stream): void
    {
        config(['filesystems.disks.s3' => ['driver' => 's3']]);

        $filesystemManagerMock = $this->createMock(FilesystemManager::class);
        $filesystemMock = $this->createMock(FilesystemAdapter::class);
        $filesystemMock->method('readStream')->willReturn($stream);
        $filesystemMock->method('getDriver')->willReturn($filesystemMock);
        $filesystemMock->method('get')->willReturn($filesystemMock);
        $filesystemManagerMock->method('disk')->with('s3')->willReturn($filesystemMock);
        $this->app['filesystem'] = $filesystemManagerMock;
    }

    /**
     * Get test image content.
     */
    protected function getTestImageContent(): string
    {
        return file_get_contents(__DIR__.'/files/test-image.jpg');
    }

    public function testGetExists()
    {
        $cache = $this->createCache();
        $url = 'abc://some/image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        copy(__DIR__.'/files/test-image.jpg', $cachedPath);
        $this->assertTrue(touch($cachedPath, time() - 1));
        $fileatime = fileatime($cachedPath);
        $this->assertNotEquals(time(), $fileatime);

        $result = $cache->get($file, fn ($file, $path) => $file);

        $this->assertInstanceof(File::class, $result);
        clearstatcache();
        $this->assertNotEquals($fileatime, fileatime($cachedPath));
    }

    public function testGetRemote()
    {
        $url = 'https://files/image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $cache = $this->createCacheWithMockClient([
            new Response(200, [], $this->getTestImageContent()),
        ]);

        $this->assertFileDoesNotExist($cachedPath);
        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
    }

    public function testGetRemoteTooLarge()
    {
        $url = 'https://files/image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $cache = $this->createCacheWithMockClient([
            new Response(200, ['Content-Length' => 100], $this->getTestImageContent()),
        ], ['max_file_size' => 1]);

        try {
            $cache->get($file, $this->noop);
            $this->fail('Expected FileIsTooLargeException to be thrown.');
        } catch (FileIsTooLargeException $exception) {
            $this->assertFileDoesNotExist($cachedPath);
        }
    }

    public function testGetDiskDoesNotExist()
    {
        $file = new GenericFile('abc://files/image.jpg');
        $cache = $this->createCache();

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Disk [abc] does not have a configured driver");
        $cache->get($file, $this->noop);
    }

    public function testGetDiskLocal()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $url = 'test://test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);
        $cache = $this->createCache();

        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
    }

    public function testGetDiskLocalDoesNotExist()
    {
        $file = new GenericFile('test://test-image.jpg');
        $cache = $this->createCache();

        $this->expectException(FileNotFoundException::class);
        $cache->get($file, $this->noop);
    }

    public function testGetDiskCloud()
    {
        $url = 's3://files/test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $stream = fopen(__DIR__.'/files/test-image.jpg', 'rb');
        $this->mockS3Filesystem($stream);

        $cache = $this->createCache();

        $this->assertFileDoesNotExist($cachedPath);
        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
        $this->assertFalse(is_resource($stream));
    }

    public function testGetDiskCloudTooLarge()
    {
        $url = 's3://files/test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $stream = fopen(__DIR__.'/files/test-image.jpg', 'rb');
        $this->mockS3Filesystem($stream);

        $cache = $this->createCache(['max_file_size' => 1]);

        try {
            $cache->get($file, $this->noop);
            $this->fail('Expected FileIsTooLargeException to be thrown.');
        } catch (FileIsTooLargeException $exception) {
            $this->assertFileDoesNotExist($cachedPath);
            $this->assertFalse(is_resource($stream));
        }
    }

    public function testGetThrowOnLock()
    {
        $cache = $this->createCache();
        $url = 'abc://some/image.jpg';
        $file = new GenericFile($url);
        $path = $this->getCachedPath($url);
        touch($path, time() - 1);

        $handle = fopen($path, 'w');
        flock($handle, LOCK_EX);

        try {
            $this->expectException(FileLockedException::class);
            $cache->get($file, fn ($file, $path) => $file, true);
        } finally {
            if (is_resource($handle)) {
                flock($handle, LOCK_UN);
                fclose($handle);
            }
        }
    }

    public function testGetIgnoreZeroSize()
    {
        $cache = $this->createCache();
        $url = 'fixtures://test-file.txt';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        touch($cachedPath);
        $this->assertEquals(0, filesize($cachedPath));

        $cache->get($file, fn ($file, $path) => $file);

        $this->assertNotEquals(0, filesize($cachedPath));
    }

    public function testGetOnce()
    {
        $url = 'fixtures://test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $cache = $this->createCache();
        $result = $cache->getOnce($file, fn ($file, $path) => $file);

        $this->assertInstanceof(File::class, $result);
        $this->assertFileDoesNotExist($cachedPath);
    }

    public function testBatch()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $url = 'test://test-image.jpg';
        $file = new GenericFile($url);
        $file2 = new GenericFile($url);
        $hash = hash('sha256', $url);

        $cache = $this->createCache();
        $paths = $cache->batch([$file, $file2], fn ($files, $paths) => $paths);

        $this->assertCount(2, $paths);
        $this->assertStringContainsString($hash, $paths[0]);
        $this->assertStringContainsString($hash, $paths[1]);
    }

    public function testBatchThrowOnLock()
    {
        $cache = $this->createCache();
        $url = 'abc://some/image.jpg';
        $file = new GenericFile($url);
        $path = $this->getCachedPath($url);
        touch($path, time() - 1);

        $handle = fopen($path, 'w');
        flock($handle, LOCK_EX);

        try {
            $this->expectException(FileLockedException::class);
            $cache->batch([$file], fn ($file, $path) => $file, true);
        } finally {
            if (is_resource($handle)) {
                flock($handle, LOCK_UN);
                fclose($handle);
            }
        }
    }

    public function testBatchOnce()
    {
        $url = 'fixtures://test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $this->createCache()->batchOnce([$file], $this->noop);

        $this->assertFileDoesNotExist($cachedPath);
    }

    public function testPrune()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        touch("{$this->cachePath}/abc", time() - 1);
        $this->app['files']->put("{$this->cachePath}/def", 'def');

        $cache = $this->createCache(['max_size' => 3]);
        $cache->prune();

        $this->assertFileDoesNotExist("{$this->cachePath}/abc");
        $this->assertFileExists("{$this->cachePath}/def");

        $cache = $this->createCache(['max_size' => 0]);
        $cache->prune();

        $this->assertFileDoesNotExist("{$this->cachePath}/def");
    }

    public function testPruneAge()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        touch("{$this->cachePath}/abc", time() - 61);
        $this->app['files']->put("{$this->cachePath}/def", 'def');

        $cache = $this->createCache(['max_age' => 1]);
        $cache->prune();

        $this->assertFileDoesNotExist("{$this->cachePath}/abc");
        $this->assertFileExists("{$this->cachePath}/def");
    }

    public function testClear()
    {
        $this->app['files']->put("{$this->cachePath}/abc", 'abc');
        $this->app['files']->put("{$this->cachePath}/def", 'abc');

        $handle = fopen("{$this->cachePath}/def", 'rb');
        flock($handle, LOCK_SH);

        try {
            $this->createCache()->clear();

            $this->assertFileExists("{$this->cachePath}/def");
            $this->assertFileDoesNotExist("{$this->cachePath}/abc");
        } finally {
            if (is_resource($handle)) {
                flock($handle, LOCK_UN);
                fclose($handle);
            }
        }
    }

    public function testMimeTypeWhitelist()
    {
        $cache = $this->createCache(['mime_types' => ['image/jpeg']]);

        // Should work for allowed MIME type
        $cache->get(new GenericFile('fixtures://test-image.jpg'), $this->noop);

        // Should throw for disallowed MIME type
        $this->expectException(MimeTypeIsNotAllowedException::class);
        $this->expectExceptionMessage('text/plain');
        $cache->get(new GenericFile('fixtures://test-file.txt'), $this->noop);
    }

    public function testExistsDisk()
    {
        $file = new GenericFile('test://test-image.jpg');
        $cache = $this->createCache();

        $this->assertFalse($cache->exists($file));
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $this->assertTrue($cache->exists($file));
    }

    public function testExistsDiskTooLarge()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $file = new GenericFile('test://test-image.jpg');
        $cache = $this->createCache(['max_file_size' => 1]);

        $this->expectException(FileIsTooLargeException::class);
        $cache->exists($file);
    }

    public function testExistsDiskMimeNotAllowed()
    {
        $this->app['files']->put("{$this->diskPath}/test-file.txt", 'abc');
        $file = new GenericFile('test://test-file.txt');
        $cache = $this->createCache(['mime_types' => ['image/jpeg']]);

        $this->expectException(MimeTypeIsNotAllowedException::class);
        $this->expectExceptionMessage('text/plain');
        $cache->exists($file);
    }

    public function testExistsRemote404()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([new Response(404)]);

        $this->assertFalse($cache->exists($file));
    }

    public function testExistsRemote404WithHttpErrorsEnabled()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([new Response(404)], [], true);

        $this->assertFalse($cache->exists($file));
    }

    public function testExistsRemote500()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([new Response(500)]);

        $this->assertFalse($cache->exists($file));
    }

    public function testExistsRemoteRetriesOnServerErrorWithHttpErrorsEnabled()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([
            new Response(500),
            new Response(200),
        ], [
            'http_retries' => 1,
            'http_retry_delay' => 1,
        ], true);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteRetriesOnServerError()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([
            new Response(500),
            new Response(200),
        ], [
            'http_retries' => 1,
            'http_retry_delay' => 1,
        ]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteRetriesOn429()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([
            new Response(429),
            new Response(200),
        ], [
            'http_retries' => 1,
            'http_retry_delay' => 1,
        ]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteRetriesOnConnectException()
    {
        $url = 'https://example.com/file';
        $file = new GenericFile($url);
        $request = new Request('HEAD', $url);
        $connectException = new ConnectException('Temporary network error', $request);

        $cache = $this->createCacheWithMockClient([
            $connectException,
            new Response(200),
        ], [
            'http_retries' => 1,
            'http_retry_delay' => 1,
        ]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteThrowsAfterConnectRetriesExhausted()
    {
        $url = 'https://example.com/file';
        $file = new GenericFile($url);
        $request = new Request('HEAD', $url);
        $connectException = new ConnectException('Temporary network error', $request);

        $cache = $this->createCacheWithMockClient([
            $connectException,
            $connectException,
        ], [
            'http_retries' => 1,
            'http_retry_delay' => 1,
        ]);

        $this->expectException(ConnectException::class);
        $this->expectExceptionMessage('Temporary network error');
        $cache->exists($file);
    }

    public function testExistsRemote200()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient([new Response(200)]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteTooLarge()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient(
            [new Response(200, ['content-length' => 100])],
            ['max_file_size' => 1]
        );

        $this->expectException(FileIsTooLargeException::class);
        $cache->exists($file);
    }

    public function testExistsRemoteMimeNotAllowed()
    {
        $file = new GenericFile('https://example.com/file');
        $cache = $this->createCacheWithMockClient(
            [new Response(200, ['content-type' => 'application/json'])],
            ['mime_types' => ['image/jpeg']]
        );

        $this->expectException(MimeTypeIsNotAllowedException::class);
        $this->expectExceptionMessage('application/json');
        $cache->exists($file);
    }

    public function testExistsRemoteTimeout()
    {
        $url = 'https://files.example.com/image.jpg';
        $file = new GenericFile($url);
        $request = new Request('HEAD', $url);
        $connectException = new ConnectException('Example of connection failed', $request);

        $cache = $this->createCacheWithMockClient([$connectException]);

        $this->expectException(ConnectException::class);
        $this->expectExceptionMessage('Example of connection failed');
        $cache->exists($file);
    }

    public function testGetRemoteThrowsConnectExceptionOnGetRequest()
    {
        $url = 'https://files.example.com/image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);
        $request = new Request('GET', $url);
        $connectException = new ConnectException('Example of connection failed', $request);

        $cache = $this->createCacheWithMockClient([$connectException]);

        try {
            $this->expectException(ConnectException::class);
            $this->expectExceptionMessage('Example of connection failed');
            $cache->get($file, $this->noop);
        } finally {
            $this->assertFileDoesNotExist($cachedPath);
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

        $cache = $this->createCache(['max_age' => 1, 'max_size' => 1000 ** 2]);
        $cache->prune();

        $this->assertFileDoesNotExist($unlockedFile, "Unlocked file should be pruned.");
        $this->assertFileExists($lockedFile, "Locked file should NOT be pruned.");

        flock($handle, LOCK_UN);
        fclose($handle);

        $cache->prune();
        $this->assertFileDoesNotExist($lockedFile, "Unlocked file should be pruned now.");
    }

    #[DataProvider('provideUrlsForEncoding')]
    public function testEncodeUrl(string $inputUrl, string $expectedUrl)
    {
        $cache = $this->createCache();
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
        $url = 'fixtures://test-image.jpg';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);
        $cache = $this->createCache();

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

    public function testMultipleReadersAccessCachedFileSimultaneously()
    {
        $url = 'fixtures://test-file.txt';
        $file = new GenericFile($url);
        $cachedPath = $this->getCachedPath($url);

        $cache = $this->createCache();
        $initialPath = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $initialPath);
        $this->assertFileExists($cachedPath);

        // Simulate the first reader holding LOCK_SH
        $reader1Handle = fopen($cachedPath, 'rb');
        $this->assertTrue(flock($reader1Handle, LOCK_SH), "Reader 1 failed to acquire LOCK_SH.");

        // The second reader tries to get the file (must successfully get LOCK_SH)
        $reader2Handle = null;
        $reader2Path = $cache->get($file, function ($file, $path) use (&$reader2Handle) {
            $reader2Handle = fopen($path, 'rb');
            $this->assertTrue(flock($reader2Handle, LOCK_SH), "Reader 2 failed to acquire LOCK_SH.");
            return $path;
        });

        $this->assertEquals($cachedPath, $reader2Path);
        $this->assertIsResource($reader2Handle);

        flock($reader1Handle, LOCK_UN);
        fclose($reader1Handle);
        flock($reader2Handle, LOCK_UN);
        fclose($reader2Handle);
    }

    public function testConfigValidationThrowsOnInvalidMaxFileSize()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('max_file_size');

        new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => -2, // Invalid: must be -1 or positive
        ]);
    }

    public function testConfigValidationThrowsOnInvalidMaxAge()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('max_age');

        new FileCache([
            'path' => $this->cachePath,
            'max_age' => 0, // Invalid: must be at least 1
        ]);
    }

    public function testConfigValidationThrowsOnInvalidLockMaxAttempts()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('lock_max_attempts');

        new FileCache([
            'path' => $this->cachePath,
            'lock_max_attempts' => 0, // Invalid: must be at least 1
        ]);
    }

    public function testAllowedHostsValidation()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['example.com', 'cdn.example.com'],
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        // Should not throw for allowed host
        $method->invoke($cache, 'https://example.com/image.jpg');
        $method->invoke($cache, 'https://cdn.example.com/image.jpg');

        // Should throw for disallowed host
        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $method->invoke($cache, 'https://evil.com/image.jpg');
    }

    public function testAllowedHostsWildcard()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['*.example.com'],
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        // Should allow subdomains
        $method->invoke($cache, 'https://cdn.example.com/image.jpg');
        $method->invoke($cache, 'https://images.cdn.example.com/image.jpg');

        // Should also allow the root domain
        $method->invoke($cache, 'https://example.com/image.jpg');

        // Should throw for different domain
        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $method->invoke($cache, 'https://notexample.com/image.jpg');
    }

    public function testAllowedHostsNullAllowsAll()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => null,
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        // Should allow any host when allowed_hosts is null
        $this->assertNull($method->invoke($cache, 'https://any-domain.com/image.jpg'));
        $this->assertNull($method->invoke($cache, 'https://another-domain.org/file.png'));
    }

    public function testGenericFileThrowsOnEmptyUrl()
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot be empty');

        new GenericFile('');
    }

    public function testEncodeUrlBrackets()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $method = new ReflectionMethod(FileCache::class, 'encodeUrl');
        $method->setAccessible(true);

        $this->assertEquals(
            'http://example.com/path%5Bwith%5D/brackets.jpg',
            $method->invoke($cache, 'http://example.com/path[with]/brackets.jpg')
        );
    }

    public function testEncodeUrlPreservesIpv6HostBrackets()
    {
        $cache = new FileCache(['path' => $this->cachePath]);
        $method = new ReflectionMethod(FileCache::class, 'encodeUrl');
        $method->setAccessible(true);

        $this->assertEquals(
            'http://[::1]/path%5Bwith%5D/brackets.jpg',
            $method->invoke($cache, 'http://[::1]/path[with]/brackets.jpg')
        );
    }

    public function testExistsRemoteIpv6Host()
    {
        $file = new GenericFile('http://[::1]/file');
        $cache = $this->createCacheWithMockClient([new Response(200)]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteUppercaseScheme()
    {
        $file = new GenericFile('HTTPS://example.com/file');
        $cache = $this->createCacheWithMockClient([new Response(200)]);

        $this->assertTrue($cache->exists($file));
    }

    public function testBatchChunking()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');

        // Create 5 files but set chunk size to 2
        $files = [];
        for ($i = 0; $i < 5; $i++) {
            $files[] = new GenericFile('test://test-image.jpg');
        }

        $cache = new FileCache([
            'path' => $this->cachePath,
            'batch_chunk_size' => 2, // Process in chunks of 2
        ]);

        $callbackCalled = false;
        $paths = $cache->batch($files, function ($receivedFiles, $receivedPaths) use (&$callbackCalled) {
            $callbackCalled = true;
            $this->assertCount(5, $receivedFiles);
            $this->assertCount(5, $receivedPaths);
            return $receivedPaths;
        });

        $this->assertTrue($callbackCalled);
        $this->assertCount(5, $paths);
    }

    public function testBatchChunkingDisabled()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');

        $files = [];
        for ($i = 0; $i < 5; $i++) {
            $files[] = new GenericFile('test://test-image.jpg');
        }

        // batch_chunk_size = -1 disables chunking
        $cache = new FileCache([
            'path' => $this->cachePath,
            'batch_chunk_size' => -1,
        ]);

        $paths = $cache->batch($files, function ($files, $paths) {
            return $paths;
        });

        $this->assertCount(5, $paths);
    }

    public function testHttpRetryOnServerError()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        // First request fails with 500, second succeeds
        $mock = new MockHandler([
            new Response(500, [], 'Server Error'),
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'http_retries' => 1,
            'http_retry_delay' => 10, // 10ms
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
    }

    public function testHttpRetryOnConnectException()
    {
        $url = 'https://files/image.jpg';
        $file = new GenericFile($url);
        $hash = hash('sha256', $url);
        $cachedPath = "{$this->cachePath}/{$hash}";
        $request = new Request('GET', $url);
        $connectException = new ConnectException('Temporary network error', $request);

        $mock = new MockHandler([
            $connectException,
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'http_retries' => 1,
            'http_retry_delay' => 10,
        ], new Client([
            'handler' => HandlerStack::create($mock),
            'http_errors' => false,
        ]));

        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
        $this->assertFileExists($cachedPath);
    }

    public function testHttpNoRetryOnClientError()
    {
        $file = new GenericFile('https://files/image.jpg');

        // 404 should not be retried - using Response (not RequestException) to test new status code handling
        $mock = new MockHandler([
            new Response(404, [], 'Not Found'),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'http_retries' => 3,
        ], new Client([
            'handler' => HandlerStack::create($mock),
            'http_errors' => false,
        ]));

        $this->expectException(FailedToRetrieveFileException::class);
        $this->expectExceptionMessage('status code 404');
        $cache->get($file, $this->noop);
    }

    public function testHttpRetryOn429RateLimit()
    {
        $file = new GenericFile('https://files/image.jpg');
        $hash = hash('sha256', 'https://files/image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        // 429 should be retried - using Response to test new status code handling
        $mock = new MockHandler([
            new Response(429, [], 'Rate limited'),
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'http_retries' => 1,
            'http_retry_delay' => 10,
        ], new Client([
            'handler' => HandlerStack::create($mock),
            'http_errors' => false,
        ]));

        $path = $cache->get($file, $this->noop);
        $this->assertEquals($cachedPath, $path);
    }

    public function testConfigValidationThrowsOnInvalidMaxSize()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('max_size');

        new FileCache([
            'path' => $this->cachePath,
            'max_size' => -1, // Invalid: must be 0 or positive
        ]);
    }

    public function testConfigValidationThrowsOnInvalidLockWaitTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('lock_wait_timeout');

        new FileCache([
            'path' => $this->cachePath,
            'lock_wait_timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('timeout');

        new FileCache([
            'path' => $this->cachePath,
            'timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidConnectTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('connect_timeout');

        new FileCache([
            'path' => $this->cachePath,
            'connect_timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidReadTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('read_timeout');

        new FileCache([
            'path' => $this->cachePath,
            'read_timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidPruneTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('prune_timeout');

        new FileCache([
            'path' => $this->cachePath,
            'prune_timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidLifecycleLockTimeout()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('lifecycle_lock_timeout');

        new FileCache([
            'path' => $this->cachePath,
            'lifecycle_lock_timeout' => -2, // Invalid: must be -1 or non-negative
        ]);
    }

    public function testConfigValidationThrowsOnInvalidBatchChunkSize()
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('batch_chunk_size');

        new FileCache([
            'path' => $this->cachePath,
            'batch_chunk_size' => 0, // Invalid: must be -1 or positive
        ]);
    }

    public function testAllowedHostsFromEnvString()
    {
        // Simulate env string format (comma-separated)
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => 'example.com, cdn.example.com, *.trusted.com',
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        // Should not throw for allowed hosts
        $method->invoke($cache, 'https://example.com/image.jpg');
        $method->invoke($cache, 'https://cdn.example.com/image.jpg');
        $method->invoke($cache, 'https://sub.trusted.com/image.jpg');

        // Should throw for disallowed host
        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $method->invoke($cache, 'https://evil.com/image.jpg');
    }

    public function testAllowedHostsEmptyAllowsAll()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => [], // Empty array should allow all
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        // Should allow any host when allowed_hosts is empty
        $this->assertNull($method->invoke($cache, 'https://any-domain.com/image.jpg'));
    }

    public function testLifecycleLockPathUsesNormalizedCachePath()
    {
        $cacheWithPlainPath = new FileCache(['path' => $this->cachePath]);
        $cacheWithTrailingSlash = new FileCache(['path' => $this->cachePath . '/']);

        $method = new ReflectionMethod(FileCache::class, 'getLifecycleLockPath');
        $method->setAccessible(true);

        $this->assertSame(
            $method->invoke($cacheWithPlainPath),
            $method->invoke($cacheWithTrailingSlash)
        );
    }

    public function testAllowedHostsThrowsOnEmptyHost()
    {
        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['example.com'],
        ]);

        $method = new ReflectionMethod(FileCache::class, 'validateHost');
        $method->setAccessible(true);

        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $this->expectExceptionMessage('(empty)');
        $method->invoke($cache, '/no-host-url');
    }

    public function testGetRemoteWithAllowedHostsValidation()
    {
        $file = new GenericFile('https://allowed.example.com/image.jpg');

        $mock = new MockHandler([
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['allowed.example.com'],
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $path = $cache->get($file, $this->noop);
        $this->assertFileExists($path);
    }

    public function testGetRemoteBlocksDisallowedHost()
    {
        $file = new GenericFile('https://blocked.example.com/image.jpg');

        $mock = new MockHandler([
            new Response(200, [], file_get_contents(__DIR__.'/files/test-image.jpg')),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['allowed.example.com'],
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $cache->get($file, $this->noop);
    }

    public function testExistsRemoteBlocksDisallowedHost()
    {
        $file = new GenericFile('https://blocked.example.com/image.jpg');

        $mock = new MockHandler([
            new Response(200),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'allowed_hosts' => ['allowed.example.com'],
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $this->expectException(\Jackardios\FileCache\Exceptions\HostNotAllowedException::class);
        $cache->exists($file);
    }

    public function testExistsRemoteMimeTypeWithCharset()
    {
        // MIME type with charset should be handled correctly
        $mock = new MockHandler([
            new Response(200, ['content-type' => 'text/plain; charset=utf-8']),
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'mime_types' => ['text/plain'],
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $file = new GenericFile('https://example.com/file.txt');
        $this->assertTrue($cache->exists($file));
    }

    public function testBatchOnceDeletesFilesAfterCallback()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $cache = new FileCache(['path' => $this->cachePath]);

        $pathInCallback = null;
        $cache->batchOnce([$file], function ($files, $paths) use (&$pathInCallback) {
            $pathInCallback = $paths[0];
            $this->assertFileExists($paths[0]);
            return $paths;
        });

        $this->assertNotNull($pathInCallback);
        $this->assertFileDoesNotExist($cachedPath);
    }

    public function testBatchOnceDeletesFilesAfterCallbackException()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $hash = hash('sha256', 'fixtures://test-image.jpg');
        $cachedPath = "{$this->cachePath}/{$hash}";

        $cache = new FileCache(['path' => $this->cachePath]);

        try {
            $cache->batchOnce([$file], function ($files, $paths) {
                $this->assertFileExists($paths[0]);
                throw new \RuntimeException('Callback failed');
            });
            $this->fail('Expected RuntimeException to be thrown.');
        } catch (\RuntimeException $exception) {
            $this->assertSame('Callback failed', $exception->getMessage());
        }

        $this->assertFileDoesNotExist($cachedPath);
    }

    public function testBatchOncePreservesPrimaryExceptionWhenCleanupFails()
    {
        $file = new GenericFile('fixtures://test-image.jpg');
        $cache = new class(['path' => $this->cachePath]) extends FileCache {
            protected function withLifecycleExclusiveLock(callable $callback)
            {
                throw new \RuntimeException('cleanup failed');
            }
        };

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('primary callback failure');

        $cache->batchOnce([$file], function () {
            throw new \RuntimeException('primary callback failure');
        });
    }

    public function testBatchOnceThrowOnLock()
    {
        $cache = $this->createCache();
        $url = 'abc://some/image.jpg';
        $file = new GenericFile($url);
        $path = $this->getCachedPath($url);
        touch($path, time() - 1);

        $handle = fopen($path, 'w');
        flock($handle, LOCK_EX);

        try {
            $this->expectException(FileLockedException::class);
            $cache->batchOnce([$file], fn ($file, $path) => $file, true);
        } finally {
            if (is_resource($handle)) {
                flock($handle, LOCK_UN);
                fclose($handle);
            }
        }
    }

    public function testPruneOnNonExistentPath()
    {
        $nonExistentPath = sys_get_temp_dir() . '/non_existent_path_' . uniqid();
        $cache = new FileCache(['path' => $nonExistentPath]);

        $this->assertSame(['deleted' => 0, 'remaining' => 0, 'total_size' => 0], $cache->prune());
    }

    public function testClearOnNonExistentPath()
    {
        $nonExistentPath = sys_get_temp_dir() . '/non_existent_path_' . uniqid();
        $cache = new FileCache(['path' => $nonExistentPath]);

        $this->assertDirectoryDoesNotExist($nonExistentPath);
        $cache->clear();
        $this->assertDirectoryDoesNotExist($nonExistentPath);
    }

    public function testRetrieveCreatesPathIfNotExists()
    {
        $newPath = sys_get_temp_dir() . '/new_cache_path_' . uniqid();
        $this->assertDirectoryDoesNotExist($newPath);

        $file = new GenericFile('fixtures://test-file.txt');
        $cache = new FileCache(['path' => $newPath]);

        try {
            $cache->get($file, $this->noop);
            $this->assertDirectoryExists($newPath);
        } finally {
            $this->app['files']->deleteDirectory($newPath);
        }
    }

    public function testGetDefaultCallback()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $cache = new FileCache(['path' => $this->cachePath]);

        // get() without callback should return path
        $result = $cache->get($file);
        $this->assertIsString($result);
        $this->assertFileExists($result);
    }

    public function testBatchDefaultCallback()
    {
        $this->app['files']->put("{$this->diskPath}/test-image.jpg", 'abc');
        $file = new GenericFile('test://test-image.jpg');

        $cache = new FileCache(['path' => $this->cachePath]);

        // batch() without callback should return array of paths
        $result = $cache->batch([$file]);
        $this->assertIsArray($result);
        $this->assertCount(1, $result);
        $this->assertFileExists($result[0]);
    }

    public function testGetOnceDefaultCallback()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $cache = new FileCache(['path' => $this->cachePath]);

        // getOnce() without callback should return path (and delete file)
        $result = $cache->getOnce($file);
        $this->assertIsString($result);
    }

    public function testBatchOnceDefaultCallback()
    {
        $file = new GenericFile('fixtures://test-file.txt');
        $cache = new FileCache(['path' => $this->cachePath]);

        // batchOnce() without callback should return array of paths
        $result = $cache->batchOnce([$file]);
        $this->assertIsArray($result);
        $this->assertCount(1, $result);
    }

    public function testUnlimitedFileSize()
    {
        $this->app['files']->put("{$this->diskPath}/large-file.txt", str_repeat('x', 10000));
        $file = new GenericFile('test://large-file.txt');

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => -1, // Unlimited
        ]);

        $path = $cache->get($file, $this->noop);
        $this->assertFileExists($path);
    }

    public function testExistsDiskUnlimitedFileSize()
    {
        $this->app['files']->put("{$this->diskPath}/large-file.txt", str_repeat('x', 10000));
        $file = new GenericFile('test://large-file.txt');

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => -1, // Unlimited
        ]);

        $this->assertTrue($cache->exists($file));
    }

    public function testExistsRemoteUnlimitedFileSize()
    {
        $mock = new MockHandler([
            new Response(200, ['content-length' => 1000000]), // 1MB
        ]);

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_file_size' => -1, // Unlimited
        ], new Client(['handler' => HandlerStack::create($mock)]));

        $file = new GenericFile('https://example.com/large-file.zip');
        $this->assertTrue($cache->exists($file));
    }

    public function testGenericFileRejectsInvalidUrl()
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('protocol');
        new GenericFile('invalid-url-without-protocol');
    }

    public function testGetDiskFileNotFound()
    {
        config(['filesystems.disks.test' => ['driver' => 'local', 'root' => $this->diskPath]]);
        $file = new GenericFile('test://non-existent-file.txt');

        $cache = new FileCache(['path' => $this->cachePath]);

        $this->expectException(FileNotFoundException::class);
        $cache->get($file, $this->noop);
    }

    public function testPruneBySizeDeletesOldestFiles()
    {
        // Create files with different access times
        $this->app['files']->put("{$this->cachePath}/old", str_repeat('a', 100));
        touch("{$this->cachePath}/old", time() - 10, time() - 10);

        $this->app['files']->put("{$this->cachePath}/new", str_repeat('b', 100));
        // new file has current atime

        clearstatcache();

        $cache = new FileCache([
            'path' => $this->cachePath,
            'max_size' => 100, // Only allow 100 bytes
            'max_age' => 60, // Don't prune by age
        ]);

        $cache->prune();

        // Old file should be deleted, new file should remain
        $this->assertFileDoesNotExist("{$this->cachePath}/old");
        $this->assertFileExists("{$this->cachePath}/new");
    }
}
