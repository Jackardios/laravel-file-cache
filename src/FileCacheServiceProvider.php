<?php

namespace Jackardios\FileCache;

use Jackardios\FileCache\Contracts\FileCache as FileCacheContract;
use Jackardios\FileCache\Console\Commands\PruneFileCache;
use Jackardios\FileCache\Listeners\ClearFileCache;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\ServiceProvider;

class FileCacheServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application events.
     */
    public function boot(Dispatcher $events): void
    {
        $this->publishes([
            __DIR__.'/config/file-cache.php' => base_path('config/file-cache.php'),
        ], 'config');

        $this->app->booted([$this, 'registerScheduledPruneCommand']);

        $events->listen('cache:clearing', ClearFileCache::class);
    }

    /**
     * Register the service provider.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/config/file-cache.php', 'file-cache');

        $this->app->singleton('file-cache', function ($app) {
            $config = $app['config']['file-cache'] ?? [];
            if (!is_array($config)) {
                $config = [];
            }

            return new FileCache($config);
        });
        $this->app->alias('file-cache', FileCacheContract::class);

        $this->app->singleton('command.file-cache.prune', function ($app) {
            return new PruneFileCache;
        });
        $this->commands('command.file-cache.prune');
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array<int, string>
     */
    public function provides(): array
    {
        return [
            'file-cache',
            'command.file-cache.prune',
        ];
    }

    /**
     * Register the scheduled command to prune the file cache.
     */
    public function registerScheduledPruneCommand(): void
    {
        $expression = config('file-cache.prune_interval', '*/5 * * * *');
        if (!is_string($expression) || $expression === '') {
            $expression = '*/5 * * * *';
        }

        $this->app->make(Schedule::class)
            ->command(PruneFileCache::class)
            ->cron($expression);
    }
}
