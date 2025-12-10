<?php

namespace Jackardios\FileCache;

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

        // Wait for Laravel to boot before adding the scheduled event.
        // See: https://stackoverflow.com/a/36630136/1796523
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
            return new FileCache($app['config']['file-cache'] ?? []);
        });

        $this->app->singleton('command.file-cache.prune', function ($app) {
            return new PruneFileCache;
        });
        $this->commands('command.file-cache.prune');
    }

    /**
     * Get the services provided by the provider.
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
        $this->app->make(Schedule::class)
            ->command(PruneFileCache::class)
            ->cron(config('file-cache.prune_interval'));
    }
}
