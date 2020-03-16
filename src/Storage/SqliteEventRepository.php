<?php
/**
 * @copyright 2019 Benjamin J. Ferge
 * @license LGPL
 */

declare(strict_types=1);

namespace EventStore\Storage;

use EventStore\Event;
use EventStore\EventStream;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

class SqliteEventRepository extends PdoEventRepository
{
    private const CREATE_TABLE_EVENTS = <<<SQL
    CREATE TABLE IF NOT EXISTS events (
        id BLOB PRIMARY KEY,
        stream_id INTEGER,
        type TEXT,
        payload JSON,
        version INTEGER,
        occured_at REAL,
        recorded_at REAL)
SQL;
    
        private const CREATE_TABLE_SNAPSHOTS = <<<SQL
    CREATE TABLE IF NOT EXISTS snapshots (
        id BLOB PRIMARY KEY,
        stream_id INTEGER,
        state JSON,
        version INTEGER)
SQL;
    
        private const CREATE_TABLE_STREAMS = <<<SQL
    CREATE TABLE IF NOT EXISTS streams (
        id BLOB PRIMARY KEY,
        type TEXT,
        version INTEGER,
        created_at REAL,
        updated_at REAL)
SQL;

    public function __construct(\PDO $pdo)
    {
        parent::__construct($pdo);
    }

    public function createStream($type): EventStream
    {
        $version = 0;
        // ? Generated 6 decimals but saves only 4
        $createdAt = round(microtime(true), 4);
        $streamId = Uuid::uuid4();
        $sql = <<<SQL
            INSERT INTO streams (id, type, version, created_at, updated_at)
            VALUES (:id, :type, :version, :created_at, :updated_at)
SQL;
        $stmt = $this->pdo->prepare($sql);
        $result = $stmt->execute([
            "id" => $streamId,
            "type" => $type,
            "version" => $version,
            "created_at" => $createdAt,
            "updated_at" => null
        ]);
        return new EventStream($streamId, $type, $version, $createdAt);
    }

    public function getStream(UuidInterface $id): EventStream
    {
        $qry = "SELECT * FROM streams WHERE id = ?";
        $stmt = $this->pdo->prepare($qry);
        $result = $stmt->execute([$id]);

        if (!$result) {
            // TODO: throw exception
        }

        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stream = $this->parseStreamFromRow($row);
        return $stream;
    }

    private function parseStreamFromRow($row)
    {
        $id = Uuid::fromString($row['id']);
        $stream = new EventStream(
            $id, 
            $row['type'], 
            (int) $row['version'], 
            (float) $row['created_at'],
            $this->getEventsByStream($id)
        );
        return $stream;
    }

    public function getStreams(): array
    {
        $qry = "SELECT * FROM streams";
        $stmt = $this->pdo->prepare($qry);
        $result = $stmt->execute();
        
        $streams = [];
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC))
        {
            $streams[] = $this->parseStreamFromRow($row);
        }
        return $streams;
    }

    public function getStreamsByType(string $type): array
    {
        $qry = "SELECT * FROM streams WHERE type = ?";
        $stmt = $this->pdo->prepare($qry);
        $result = $stmt->execute([$type]);
        
        $streams = [];
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC))
        {
            $streams[] = $this->parseStreamFromRow($row);
        }
        return $streams;
    }

    public function getEvents(): array
    {
        $qry = "SELECT * FROM events";
        $stmt = $this->pdo->prepare($qry);

        // TODO: Error checking
        $result = $stmt->execute();
        
        $events = [];
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC))
        {
            $events[] = new Event(
                $row['type'], 
                json_decode($row['payload'], true), 
                (int) $row['version'], 
                Uuid::fromString($row['id']), 
                Uuid::fromString($row['stream_id']), 
                (float) $row['occured_at'], 
                (float) $row['recorded_at']
            );
        }
        return $events;
    }

    public function deleteEvent($eventId)
    {
        $qry = "DELETE FROM events WHERE id = ?";
        $stmt = $this->pdo->prepare($qry);

        $stmt->execute([(string) $eventId]);
    }

    public function getVersionForStream($streamId): int
    {
        $sql = <<<SQL
            SELECT version FROM streams
            WHERE id = :streamId
SQL;
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(["streamId" => $streamId]);
        $version = (int)$stmt->fetchColumn();
        return $version;
    }

    public function getEventsByStream(UuidInterface $streamId): array
    {
        $qry = "SELECT * FROM events WHERE stream_id = ?";
        $stmt = $this->pdo->prepare($qry);
        $result = $stmt->execute([$streamId]);
        if (!$result) {
        }
        $events = [];
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $event = new Event(
                $row["type"],
                json_decode($row["payload"], true),
                (int)$row["version"],
                Uuid::fromString($row["id"]),
                $streamId,
                (float)$row["occured_at"],
                (float)$row["recorded_at"]
            );
            $events[] = $event;
        }
        return $events;
    }

    public function push(Event $event)
    {
        $sql = <<<SQL
            INSERT INTO events (id, stream_id, type, payload, version, occured_at)
            VALUES (:eventId, :streamId, :type, :jsonPayload, :version, :occuredAt)
SQL;

        $stmt = $this->pdo->prepare($sql);
        $occuredAt = $event->occuredAt;
        $event->recordedAt = microtime(true);
        $recordedAt = $event->recordedAt;
        $payload = $event->payload;
        $jsonPayload = json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        $eventId = $event->id;
        $streamId = $event->streamId;
        $type = $event->type;
        $version = $event->version;
        $stmt->execute(
            compact("eventId", "streamId", "type", "jsonPayload", "version", "occuredAt")
        );
        return $event;
    }

    public function incrementStream($streamId, $nextVersion)
    {
        $sql = <<<SQL
            UPDATE streams SET version = :nextVersion
            WHERE id = :streamId
SQL;
    
        $stmt = $this->pdo->prepare($sql);
        $result = $stmt->execute([
            "nextVersion" => $nextVersion,
            "streamId" => $streamId
        ]);
        return $result;
    }

    public function ensureTables(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec(self::CREATE_TABLE_EVENTS);
            $this->pdo->exec(self::CREATE_TABLE_STREAMS);
            $this->pdo->exec(self::CREATE_TABLE_SNAPSHOTS);
            $this->pdo->commit();
        } catch (\PDOException $ex) {
            $this->pdo->rollBack();
            throw $ex;
        }
    }
}
