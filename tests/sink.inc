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
}
