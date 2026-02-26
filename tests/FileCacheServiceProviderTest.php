<?php

namespace Jackardios\FileCache\Tests;

use Illuminate\Console\Scheduling\Schedule;

class FileCacheServiceProviderTest extends TestCase
{
    public function testScheduledCommand()
    {
        config(['file-cache.prune_interval' => '*/5 * * * *']);
        $schedule = $this->app[Schedule::class];

        $event = null;
        foreach ($schedule->events() as $scheduledEvent) {
            if (str_contains($scheduledEvent->command, 'prune-file-cache')) {
                $event = $scheduledEvent;
                break;
            }
        }

        $this->assertNotNull($event, 'Scheduled prune-file-cache command was not registered.');
        $this->assertStringContainsString('prune-file-cache', $event->command);
        $this->assertEquals('*/5 * * * *', $event->expression);
    }
}
