<?php
// A minimal event loop for the tests, on the extension's contract: slots
// (getSlot/park/unpark) given to a phasync\ext\Poller, a sleep timer, and the
// Poller's poll() as the one blocking call. It counts parks, and records the
// timeouts park() was given.

final class LoopTimeout extends Exception {}
final class LoopCancelled extends Exception {}

final class Loop
{
    public int $parks = 0;
    public array $parkTimeouts = [];   // what park() was given (null: none)
    public int $sleeps = 0;
    public array $sleepCalls = [];     // the microseconds asked for
    public bool $instantSleep = false; // record sleeps, don't wait
    public ?string $parkThrows = null; // park() throws this class at once
    public ?Closure $onPark = null;    // fn(int $slot, ?float $timeout): true = return at once (a spurious wake), or throw
    public bool $reverse = false;      // resume what became ready in reverse order
    public \phasync\ext\Poller $poller;

    private int $nextSlot = 0;
    private array $parked = [];        // slot => Fiber
    private array $deadlines = [];     // slot => microtime deadline
    private array $ready = [];         // [Fiber, 'start' | null | Throwable]
    private array $timers = [];        // [microtime, Fiber]

    public function __construct()
    {
        $this->poller = new \phasync\ext\Poller($this->getSlot(...), $this->park(...), $this->unpark(...));
    }

    public function getSlot(): int
    {
        return $this->nextSlot++;
    }

    public function park(int $slot, float $timeout): void
    {
        if (isset($this->parked[$slot])) {
            throw new LogicException("Slot $slot is taken");
        }
        $this->parks++;
        $this->parkTimeouts[] = $timeout >= PHP_FLOAT_MAX ? null : $timeout;
        if ($this->parkThrows !== null) {
            throw new ($this->parkThrows)();
        }
        if ($this->onPark && ($this->onPark)($slot, end($this->parkTimeouts)) === true) {
            return;
        }
        $this->parked[$slot] = Fiber::getCurrent();
        if ($timeout < PHP_FLOAT_MAX) {
            $this->deadlines[$slot] = microtime(true) + $timeout;
        }
        Fiber::suspend();
    }

    public function unpark(int $slot): bool
    {
        if (!isset($this->parked[$slot])) {
            return false;
        }
        $this->ready[] = [$this->parked[$slot], null];
        unset($this->parked[$slot], $this->deadlines[$slot]);
        return true;
    }

    public function sleep(int $microseconds): void
    {
        $this->sleeps++;
        $this->sleepCalls[] = $microseconds;
        if ($this->instantSleep) {
            return;
        }
        $this->timers[] = [microtime(true) + $microseconds / 1e6, Fiber::getCurrent()];
        Fiber::suspend();
    }

    /** Throw $e into a parked coroutine, as a cancellation does. */
    public function cancel(Fiber $fiber, Throwable $e = new LoopCancelled('cancelled')): void
    {
        foreach ($this->parked as $slot => $f) {
            if ($f === $fiber) {
                unset($this->parked[$slot], $this->deadlines[$slot]);
            }
        }
        foreach ($this->timers as $k => [, $f]) {
            if ($f === $fiber) {
                unset($this->timers[$k]);
            }
        }
        $this->ready[] = [$fiber, $e];
    }

    public function go(Closure|Fiber $fn): Fiber
    {
        $fiber = $fn instanceof Fiber ? $fn : new Fiber($fn);
        $this->ready[] = [$fiber, 'start'];
        return $fiber;
    }

    /** Run until every coroutine is done (a watchdog fails a hung test instead of hanging it). */
    public function run(float $watchdog = 30): void
    {
        $end = microtime(true) + $watchdog;
        while (true) {
            while ($this->ready) {
                [$fiber, $how] = array_shift($this->ready);
                if ($how === 'start') {
                    $fiber->start();
                } elseif ($how instanceof Throwable) {
                    $fiber->throw($how);
                } else {
                    $fiber->resume();
                }
            }
            if (!$this->parked && !$this->timers) {
                return;
            }
            if (microtime(true) > $end) {
                throw new RuntimeException('Loop watchdog: ' . count($this->parked) . ' coroutine(s) still parked');
            }
            $now = microtime(true);
            $next = $now + 0.5;
            foreach ($this->deadlines as $d) {
                $next = min($next, $d);
            }
            foreach ($this->timers as [$t]) {
                $next = min($next, $t);
            }
            $ready = count($this->ready);
            $this->poller->poll(max(0, $next - $now));
            if ($this->reverse) {
                $this->ready = array_merge(array_slice($this->ready, 0, $ready), array_reverse(array_slice($this->ready, $ready)));
            }
            $now = microtime(true);
            foreach ($this->timers as $k => [$t, $fiber]) {
                if ($t <= $now) {
                    unset($this->timers[$k]);
                    $this->ready[] = [$fiber, null];
                }
            }
            foreach ($this->deadlines as $slot => $d) {
                if ($d <= $now) {
                    $this->ready[] = [$this->parked[$slot], new LoopTimeout('timed out')];
                    unset($this->parked[$slot], $this->deadlines[$slot]);
                }
            }
        }
    }

    public function manage(Closure $task, string $timeoutClass = LoopTimeout::class): mixed
    {
        return \phasync\ext\manage($task, $this->poller, $this->sleep(...), $timeoutClass);
    }

    /** Run the coroutines inside manage() until all are done. */
    public function runAll(Closure|Fiber ...$coroutines): void
    {
        $this->manage(function () use ($coroutines) {
            foreach ($coroutines as $c) {
                $this->go($c);
            }
            $this->run();
        });
    }
}
