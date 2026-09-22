<?php

/**
 * @generate-class-entries
 * @undocumentable
 */

namespace phasync;

/**
 * Like the built-in stream_select(), but the descriptor set grows on demand,
 * so it is not bounded by FD_SETSIZE (the ~1024 ceiling) on any PHP version.
 * Accepts ordinary stream resources, including the select-only sentinels
 * returned by the stream hooks.
 */
function stream_select(?array &$read, ?array &$write, ?array &$except, ?int $seconds, ?int $microseconds = null): int|false {}
