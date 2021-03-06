<?php
/**
 * @copyright 2019 Benjamin J. Ferge
 * @license LGPL
 */

declare(strict_types=1);

namespace EventStore;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

class Projection
{
    const STATUS_OK = 0;
    const STATUS_BROKEN = 1;
    const STATUS_STALLED = 2;
    const STATUS_READY = 3;

    const VERBOSE = 1;

    private $id;
    private $projection;
    private $position;
    private $status;
    private $state;
    private $eventStream;
    private $streamId;
    private $verbose;
    private $handlers = [];
    private $streamType;
    private $separate = false;

    /**
     * Creates a projection for an event stream.
     */
    public function __construct(
            EventStream $eventStream = null,
            string $streamType = null,
            array $handlers = [],
            $id = null,
            $state = null,
            int $position = 0,
            int $options = null
        ) {
        $this->position = $position;
        $this->status = self::STATUS_READY;
        $this->id = $id ?? Uuid::uuid4();
        $this->state = $state;
        $this->eventStream = $eventStream;
        $this->verbose = $options & self::VERBOSE;
        $this->handlers = $handlers;
        $this->streamType = $streamType;
    }

    public function getId(): UuidInterface
    {
        return $this->id;
    }

    public function __invoke(Event $e)
    {
        if (!isset($this->handlers[$e->type])) {
            // TODO: throw exception or at least a notice
            return;
        }
        if ($this->verbose) {
            echo "Projecting ".$e->getType()."\n";
        }
        
        $this->state = ($this->handlers[$e->type])($this->state, $e);
        $this->position++;
    }

    public function getStatus()
    {
        return $this->status;
    }

    public function getState()
    {
        return $this->state;
    }

    public function setState(array $state)
    {
        $this->state = $state;
        return $this;
    }

    public function getEventStream()
    {
        return $this->eventStream;
    }

    public function getStreamId()
    {
        return $this->streamId;
    }
    
    public function getPosition()
    {
        return $this->position;
    }

    public function getStreamType()
    {
        return $this->streamType;
    }

    public function isSeparate(): bool
    {
        return $this->separate;
    }

    public static function fromType(string $streamType)
    {
        $self = new self;
        $self->streamType = $streamType;
        return $self;
    }

    public static function fromStream(UuidInterface $streamId)
    {
        $self = new self;
        $self->streamId = $streamId;
        return $self;
    }

    public static function fromEach(string $streamType)
    {
        $self = self::fromType($streamType);
        $self->separate = true;
        return $self;
    }

    public function on(string $eventType, callable $handler)
    {
        $this->handlers[$eventType] = $handler;
        return $this;
    }

    public function init(array $initState = [])
    {
        $this->state = $initState;
        return $this;
    }

    public function exec(EventStore $eventStore)
    {
        return $eventStore->exec($this);
    }

    public function getHandlers()
    {
        return $this->handlers;
    }
}
