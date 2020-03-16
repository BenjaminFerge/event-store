<?php
/**
 * @copyright 2019 Benjamin J. Ferge
 * @license LGPL
 */

declare(strict_types=1);

namespace EventStore;

use Ramsey\Uuid\UuidInterface;
use EventStore\Storage\SqliteEventRepository;

class EventStore
{
    private $projections = [];
    private $typeProjections = [];
    private $repo;

    public function __construct($filename = ":memory:")
    {
        $pdo = new \PDO("sqlite:" . $filename);
        $pdo->setAttribute(
            \PDO::ATTR_ERRMODE,
            \PDO::ERRMODE_EXCEPTION
        );

        $repo = new SqliteEventRepository($pdo);
        $this->repo = $repo;
    }

    public function createStream($type): EventStream
    {
        return $this->repo->createStream($type);
    }

    public function getStream(UuidInterface $id): EventStream
    {
        return $this->repo->getStream($id);
    }

    public function getStreams(): array
    {
        return $this->repo->getStreams();
    }
    
    public function getStreamsByType(string $type): array
    {
        return $this->repo->getStreamsByType($type);
    }

    public function push(UuidInterface $streamId, Event $event): Event
    {
        $event->streamId = $streamId;
        $expectedVersion = $event->version - 1;
        $streamVersion = $this->repo->getVersionForStream($streamId);

        if ($streamVersion !== $expectedVersion) {
            throw new ConcurrencyException("StreamVersion ($streamVersion) is not equal with ExpectedVersion ($expectedVersion)");
        }

        $event = $this->repo->push($event);

        $nextVersion = $streamVersion + 1;

        $this->repo->incrementStream($streamId, $nextVersion);
        $this->publish($event);
        return $event;
    }

    public function rollback(UuidInterface $streamId, Event $event)
    {
        $streamVersion = $this->repo->getVersionForStream($streamId);
        $lastEventVersion = $event->version;

        if ($streamVersion == $lastEventVersion) {
            
            $this->repo->deleteEvent((string)$event->id);

            $this->repo->incrementStream($streamId, $streamVersion - 1);
        }
    }

    public function addProjection(Projection $projection): void
    {
        $this->projections[$projection->getEventStream()->id] = $projection;
    }

    public function addTypeProjection(string $type, Projection $projection): void
    {
        if (!isset($this->typeProjections[$type])) {
            $this->typeProjections[$type] = [];
        }
        $this->typeProjections[$type][] = $projection;
    }

    public function replayAll()
    {
        $events = $this->repo->getEvents();
        foreach ($events as $e) {
            $this->publish($e);
        }
    }

    public function exec(Projection $projection)
    {
        // By StreamId
        if ($projection->getStreamId() !== null) {
            foreach ($this->repo->getEvents($projection->getStreamId()) as $e) {
                ($projection)($e);
            }
            return $projection->getState();
        }

        // By StreamType and separately
        if ($projection->isSeparate()) {
            $results = [];
            $streams = $this->getStreamsByType($projection->getStreamType());
            foreach ($streams as $s) {
                $_projection = clone $projection;
                foreach ($s->events as $e) {
                    ($_projection)($e);
                }
                $results[(string) $s->id] = $_projection->getState();
            }
            return $results;
        }

        // By StreamType
        $streams = $this->getStreamsByType($projection->getStreamType());
        foreach ($streams as $s) {
            foreach ($s->events as $e) {
                ($projection)($e);
            }
        }
        return $projection->getState();
    }

    private function publish(Event $e): void
    {
        $stream = $this->getStream($e->streamId);
        $streamType = $stream->type;
        $streamId = (string) $e->streamId;
        $hasStreamProjection = false;
        $hasTypeProjection = false;
        if (isset($this->projections[$streamId])) {
            $hasStreamProjection = true;
            foreach ($this->projections[$streamId] as $p) {
                ($p)($e);
            }
        }
        if (isset($this->typeProjections[$streamType])) {
            $hasTypeProjection = true;
            foreach ($this->typeProjections[$streamType] as $p) {
                ($p)($e);
            }
        }
        if (!$hasStreamProjection || !$hasTypeProjection) {
            // TODO: Notice user
        }
    }
}
