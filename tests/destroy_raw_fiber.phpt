--TEST--
Failsafe without the loop's help: raw fibers suspended through the extension and destroyed (refcount, cycle collector) while the loop still has entries for them; the extension settles what it held for them itself and never unparks a slot of a dead fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
phasync.fs_offload=all
--FILE--
<?php
// A loop that knows its coroutines only by WeakReference and never forgets a
// slot: nothing tells it a fiber was destroyed, as if phasync's teardown never
// ran. unpark() records any slot whose fiber is gone.
final class RawLoop
{
    public \phasync\ext\Poller $poller;
    public array $parked = [];     // slot => WeakReference, never cleaned up
    public array $stale = [];      // unpark()s of slots whose fiber is gone
    private array $ready = [];
    private int $next = 0;

    public function __construct()
    {
        $this->poller = new \phasync\ext\Poller(fn() => $this->next++, $this->park(...), $this->unpark(...));
    }
    public function park(int $slot, float $timeout): void
    {
        $this->parked[$slot] = WeakReference::create(Fiber::getCurrent());
        Fiber::suspend();
    }
    public function unpark(int $slot): bool
    {
        $f = isset($this->parked[$slot]) ? $this->parked[$slot]->get() : null;
        if (isset($this->parked[$slot]) && $f === null) {
            $this->stale[] = $slot;
        }
        unset($this->parked[$slot]);
        if ($f) {
            $this->ready[] = $f;
        }
        return $f !== null;
    }
    public function sleep(int $us): void
    {
        $this->park($this->next++, $us / 1e6);   // woken by nobody: these fibers get destroyed
    }
    /** Poll and resume for a while. */
    public function spin(float $seconds): void
    {
        $end = microtime(true) + $seconds;
        do {
            $this->poller->poll(0.01);
            while ($f = array_shift($this->ready)) {
                $f->resume();
            }
        } while (microtime(true) < $end);
    }
}

function drop(string $how, Closure $fn): void
{
    $holder = new stdClass;
    $f = new Fiber(function () use ($fn, $holder) {
        try { $fn(); } finally { echo "  finally ran\n"; }
    });
    if ($how === 'gc') {
        $holder->f = $f;
    }
    $f->start();
    unset($f, $holder);
    gc_collect_cycles();
}

$file = tempnam(sys_get_temp_dir(), 'phasync_raw_');
file_put_contents($file, str_repeat('z', 1 << 24));
$loop = new RawLoop;
\phasync\ext\manage(function () use ($loop, $file) {
    foreach (['refcount', 'gc'] as $how) {
        echo "socket read, then data and a close ($how)\n";
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        drop($how, fn() => fread($a, 100));
        fwrite($b, 'x');
        $loop->spin(0.05);
        $r = new Fiber(function () use ($a) { echo "  then read: ", fread($a, 100), "\n"; });
        $r->start();
        $loop->spin(0.05);
        fclose($a);
        fclose($b);

        echo "pool read, its thread finishing later ($how)\n";
        $fp = fopen($file, 'r');
        drop($how, fn() => fread($fp, 1 << 24));
        $loop->spin(0.3);
        fclose($fp);

        echo "sleep ($how)\n";
        drop($how, fn() => usleep(100000));
        $loop->spin(0.05);

        echo "closer dropped while a reader waits ($how)\n";
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $reader = new Fiber(function () use ($a) { $r = fread($a, 100); echo "  reader: ", json_encode($r), "\n"; });
        $reader->start();
        drop($how, fn() => fclose($a));
        $loop->spin(0.05);
        echo "  stream: ", get_resource_type($a), "\n";
        fclose($b);
    }
}, $loop->poller, $loop->sleep(...), Exception::class);
echo "stale unparks: ", count($loop->stale), "\n";
unlink($file);
?>
--EXPECT--
socket read, then data and a close (refcount)
  finally ran
  then read: x
pool read, its thread finishing later (refcount)
  finally ran
sleep (refcount)
  finally ran
closer dropped while a reader waits (refcount)
  reader: false
  finally ran
  stream: Unknown
socket read, then data and a close (gc)
  finally ran
  then read: x
pool read, its thread finishing later (gc)
  finally ran
sleep (gc)
  finally ran
closer dropped while a reader waits (gc)
  reader: false
  finally ran
  stream: Unknown
stale unparks: 0