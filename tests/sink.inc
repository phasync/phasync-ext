<?php
// A $sapi for phasync\ext\virtualize() that records what a SAPI would receive.
class Sink
{
    public string $out = '';
    public ?array $headers = null;     // [status, statusLine, lines] from send_headers()
    public int $flushes = 0;
    public bool $gone = false;         // ub_write() reports the client gone
    public string $body = '';          // served by read_post()
    public array $info = [];           // returned by request_info()
    public ?string $cookie = null;     // returned by read_cookies()
    public array $server = [];         // returned by register_server_variables()
    public ?Closure $onWrite = null;   // runs before each ub_write() records (may suspend)

    public function ub_write(string $data): bool
    {
        if ($this->onWrite) ($this->onWrite)($data);
        $this->out .= $data;
        return !$this->gone;
    }

    public function send_headers(int $status, ?string $statusLine, array $headers): void
    {
        $this->headers = [$status, $statusLine, $headers];
    }

    public function flush(): void { $this->flushes++; }

    public function read_post(int $length): string
    {
        $r = substr($this->body, 0, $length);
        $this->body = (string) substr($this->body, $length);
        return $r;
    }

    public function request_info(): array { return $this->info; }

    public function read_cookies(): ?string { return $this->cookie; }

    public function register_server_variables(): array { return $this->server; }

    // A request whose Cookie header carries this session id.
    public static function withSession(?string $id): static
    {
        $s = new static;
        $s->cookie = $id === null ? null : "PHPSESSID=$id";
        return $s;
    }
}

// A request's own global variables: the superglobals and $_SESSION. virtualize()
// leaves global variables to the server, which swaps them around each resume of
// the request's fibers, as here: enter() before, leave() after.
final class RequestGlobals
{
    private array $own = [];       // the request's, while it isn't running
    private array $outer = [];     // what it replaced, while it is

    public function enter(): void
    {
        $this->outer = [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST, &$_SESSION];
        unset($_SESSION);          // $_SESSION: undefined, not null, without a session
        if ($this->own) {
            [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST] = $this->own;
            if (isset($this->own[6])) {
                $_SESSION = &$this->own[6];   // the reference ext/session holds
            }
        }
    }

    public function leave(): void
    {
        $this->own = [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST, &$_SESSION];
        unset($_SESSION);
        [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST] = $this->outer;
        if (isset($this->outer[6])) {
            $_SESSION = &$this->outer[6];
        }
        $this->outer = [];
    }

    /** $fiber->start() or ->resume(), with the request's globals. */
    public function run(Fiber $fiber): void
    {
        $this->enter();
        try {
            $fiber->isStarted() ? $fiber->resume() : $fiber->start();
        } finally {
            $this->leave();
        }
    }
}
