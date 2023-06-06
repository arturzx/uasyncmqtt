import asyncio

import time


DEFAULT_PORT = 1883
DEFAULT_SSL_PORT = 8883


class MQTTMessage:
    __slots__ = ("_topic", "_payload", "_retain", "_qos")

    def __init__(
            self,
            topic: str,
            payload: bytes,
            qos: int = 0,
            retain: bool = False
    ):
        assert 0 <= qos <= 2
        self._topic = topic
        self._payload = payload
        self._qos = qos
        self._retain = retain

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def qos(self) -> int:
        return self._qos

    @property
    def retain(self) -> bool:
        return self._retain


class MQTTMessageQueue:
    def __init__(self, size: int):
        self._queue: list[MQTTMessage | None] = [None] * size
        self._write_index = 0
        self._read_index = 0
        self._event = asyncio.Event()
        self._discards = 0

    def put(self, message: MQTTMessage):
        self._queue[self._write_index] = message
        self._event.set()
        self._write_index = (self._write_index + 1) % len(self._queue)
        if self._write_index == self._read_index:
            self._read_index = (self._read_index + 1) % len(self._queue)
            self._discards += 1

    def __aiter__(self):
        return self

    async def __anext__(self) -> MQTTMessage:
        if self._read_index == self._write_index:
            self._event.clear()
            await self._event.wait()
        message = self._queue[self._read_index]
        self._read_index = (self._read_index + 1) % len(self._queue)
        return message


class MQTTException(Exception):
    pass


class MQTTConnectionError(MQTTException):
    pass


class MQTTConnectionInterrupted(MQTTConnectionError):
    pass


class MQTTConnectionRefusedException(Exception):
    def __init__(self, code: int):
        self.reason = code


class MQTTClient:
    def __init__(
            self,
            client_id: str,
            host: str,
            port: int | None = None,
            username: str | None = None,
            password: str | None = None,
            keep_alive: int | None = None,
            last_will: MQTTMessage | None = None,
            queue_size: int = 1
    ):
        if not 0 < len(client_id) < 23:
            raise ValueError('client_id length must be in 1-23 length range')
        if keep_alive is not None and keep_alive >= 65536:
            raise ValueError('keep_alive must be in 0-65536 range')
        self._client_id = client_id
        self._host = host
        self._port = port or DEFAULT_PORT
        self._username = username
        self._password = password
        self._keep_alive = keep_alive
        self._last_will: MQTTMessage | None = last_will
        self._queue = MQTTMessageQueue(queue_size)
        self._connected: bool = False
        self._connected_events: list[asyncio.Event] = []
        self._disconnected_events: list[asyncio.Event] = []
        self._reader = None
        self._writer = None
        self._message_receiver_task: asyncio.Task | None = None
        self._connection_supervisor_task: asyncio.Task | None = None
        self._message_id = 0
        self._response_events: dict[int, asyncio.Event] = {}
        self._responses: dict[int, tuple[int, bool, int, bool, bytes] | None] = {}
        self._last_sent_message_timestamp = None
        self._last_received_message_timestamp = None
        self._last_ping_response_message_timestamp = None

    @property
    def connected(self) -> bool:
        return self._connected

    def create_connected_event(self) -> asyncio.Event:
        event = asyncio.Event()
        self._connected_events.append(event)
        return self._create_event(self._connected_events)

    def remove_connected_event(self, event: asyncio.Event):
        try:
            self._connected_events.remove(event)
        except ValueError:
            raise ValueError('Not connected event')

    def create_disconnected_event(self) -> asyncio.Event:
        event = asyncio.Event()
        self._disconnected_events.append(event)
        return self._create_event(self._disconnected_events)

    def remove_disconnected_event(self, event: asyncio.Event):
        try:
            self._disconnected_events.remove(event)
        except ValueError:
            raise ValueError('Not disconnected event')

    async def connect(self, wait: bool = False):
        if self._connection_supervisor_task is not None:
            raise MQTTException('Already connected')
        if wait:
            await self._connect(True)
        self._connection_supervisor_task = asyncio.create_task(self._connection_supervisor())

    async def disconnect(self):
        if self._connection_supervisor_task is None:
            raise MQTTException('Not connected')
        self._connection_supervisor_task.cancel()
        try:
            await self._connection_supervisor_task
        except asyncio.CancelledError:
            pass
        self._connection_supervisor_task = None
        if self._writer is not None:
            await self._send_message(14)
            await self._disconnect()

    async def publish(self, topic: str, payload: bytes, retain: bool = False, qos: int = 0):
        if self._connected is False:
            raise MQTTException('Not connected')
        await self._send_message(
            3,
            None,
            2 + len(topic) + (2 if qos > 0 else 0) + len(payload),
            False,
            qos,
            retain
        )
        self._send_string(topic.encode())
        message_id = None
        if qos > 0:
            message_id = self._send_message_id()
        self._writer.write(payload)
        await self._writer.drain()
        if qos == 1:
            response_type, _, _, _, _ = await self._wait_for_response(message_id)
        elif qos == 2:
            raise NotImplementedError("Only QoS 0 and 1 supported")

    async def subscribe(
            self,
            topic_or_topics: str | tuple[str | tuple[str, int], ...] | list[str | tuple[str, int]] | set[str | tuple[str, int]],
            qos: int = 0
    ) -> int | tuple[tuple[str, int]]:
        assert 0 <= qos <= 2
        if self._connected is False:
            raise MQTTException('Not connected')
        continuation_length = 2
        if isinstance(topic_or_topics, str):
            continuation_length += 2 + len(topic_or_topics) + 1
        else:
            for topic_or_topic_with_qos in topic_or_topics:
                if isinstance(topic_or_topic_with_qos, str):
                    continuation_length += 2 + len(topic_or_topic_with_qos) + 1
                else:
                    assert 0 <= topic_or_topic_with_qos[1] <= 2
                    continuation_length += 2 + len(topic_or_topic_with_qos[0]) + 1
        await self._send_message(
            8,
            None,
            continuation_length,
            False,
            1,
            False
        )
        message_id = self._send_message_id()
        if isinstance(topic_or_topics, str):
            self._send_string(topic_or_topics.encode())
            self._send_qos(qos)
        else:
            for topic_or_topic_with_qos in topic_or_topics:
                if isinstance(topic_or_topic_with_qos, str):
                    self._send_string(topic_or_topic_with_qos.encode())
                    self._send_qos(qos)
                else:
                    assert 0 <= topic_or_topic_with_qos[1] <= 2
                    self._send_string(topic_or_topic_with_qos[0].encode())
                    self._send_qos(topic_or_topic_with_qos[1])
        await self._writer.drain()
        response_type, _, _, _, response_payload = await self._wait_for_response(message_id)
        if len(response_payload) == 1:
            return int.from_bytes(response_payload, 'big')
        else:
            return tuple(
                (topic_or_topics[i] if isinstance(topic_or_topics[i], str) else topic_or_topics[i][0], qos)
                for i, qos in enumerate(response_payload)
            )

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._queue.__anext__()

    @staticmethod
    def _create_event(events_list: list[asyncio.Event]):
        event = asyncio.Event()
        events_list.append(event)
        return event

    async def _connect(self, clean_session: bool = False):
        self._connected = False
        self._message_id = 0
        self._response_events.clear()
        self._responses.clear()
        self._last_sent_message_timestamp = None
        self._last_received_message_timestamp = None
        self._last_ping_response_message_timestamp = None
        try:
            # TODO add configurable timeout
            self._reader, self._writer = await asyncio.wait_for(asyncio.open_connection(self._host, self._port), 5.0)
        except asyncio.TimeoutError:
            raise MQTTConnectionError("Connect timeout")
        packet = bytearray(
            b"\x00\x04"  # Protocol name length
            b"MQTT"  # Protocol name
            b"\x04"  # Protocol version (3.1.1)
            b"\x00"  # Connect Flags
            b"\x00\x00"  # Keep alive timer
        )
        continuation_length = 2 + len(self._client_id)
        # Flags byte
        if self._username is not None:
            packet[0x07] |= 1 << 7
            continuation_length += 2 + len(self._username)
        if self._password is not None:
            packet[0x07] |= 1 << 6
            continuation_length += 2 + len(self._password)
        if self._last_will is not None:
            packet[0x07] |= self._last_will.retain << 5
            packet[0x07] |= 0x4 | (self._last_will.qos & 0x1) << 3 | (self._last_will.qos & 0x2) << 3
            packet[0x07] |= 1 << 2
            continuation_length += 2 + len(self._last_will.topic) + 2 + len(self._last_will.payload)
        packet[0x07] |= clean_session << 1
        # Keep alive timer bytes
        packet[0x08] = self._keep_alive >> 8
        packet[0x09] = self._keep_alive & 0x00FF
        await self._send_message(1, packet, continuation_length)
        self._send_string(self._client_id.encode())
        if self._last_will is not None:
            self._send_string(self._last_will.topic.encode())
            self._send_string(self._last_will.payload)
        if self._username is not None:
            self._send_string(self._username.encode())
        if self._password is not None:
            self._send_string(self._password.encode())
        await self._writer.drain()
        response_type, _, _, _, response_payload = await self._receive_message()
        if response_type != 2:  # CONNACK
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
            raise MQTTConnectionError('Received invalid response for CONNECT packet')
        if response_payload[0x01] != 0x00:
            raise MQTTConnectionRefusedException(response_payload[0x01])
        self._last_sent_message_timestamp = time.time()
        self._message_receiver_task = asyncio.create_task(self._message_receiver())
        self._connected = True
        for event in self._connected_events:
            event.set()

    async def _disconnect(self):
        self._message_receiver_task.cancel()
        try:
            await self._message_receiver_task
        except asyncio.CancelledError:
            pass
        self._message_receiver_task = None
        self._writer.close()
        # TODO add wait_for?
        await self._writer.wait_closed()
        self._writer = None
        self._reader = None
        self._connected = False
        for event in self._disconnected_events:
            event.set()

    async def _send_message(
            self, type_: int,
            payload: bytes | None = None,
            continuation_length: int = 0,
            dup: bool = False,
            qos: int = 0,
            retain: bool = False
    ):
        assert 0 <= qos <= 2
        header = bytearray(6)
        header[0x00] = type_ << 4 | dup << 3 | qos << 1 | retain
        length = (len(payload) if payload is not None else 0) + continuation_length
        i = 1
        while length > 0x7F:
            header[i] = (length & 0x7F) | 0x80
            length >>= 7
            i += 1
        header[i] = length
        self._writer.write(header[:i + 1])
        if payload is not None:
            self._writer.write(payload)
        if continuation_length == 0:
            await self._writer.drain()
        self._last_sent_message_timestamp = time.time()

    def _send_message_id(self) -> int:
        if self._message_id >= 65535:
            self._message_id = 0
        self._message_id += 1
        message_id = self._message_id
        event = asyncio.Event()
        self._response_events[message_id] = event
        self._writer.write(message_id.to_bytes(2, 'big'))
        return message_id

    def _send_qos(self, qos: int):
        self._writer.write(qos.to_bytes(1, "big"))

    def _send_string(self, data: bytes):
        self._writer.write(len(data).to_bytes(2, 'big'))
        self._writer.write(data)

    async def _receive_length(self) -> int | None:
        n = 0
        shift = 0
        while True:
            b = await self._reader.read(1)
            if len(b) == 0:
                raise MQTTConnectionInterrupted
            n |= (b[0] & 0x7F) << shift
            if not b[0] & 0x80:
                return n
            shift += 7

    async def _receive_message(self) -> tuple[int, bool, int, bool, bytes | None]:
        b = await self._reader.read(1)
        if len(b) == 0:
            raise MQTTConnectionInterrupted
        type_ = b[0] >> 4
        dup = bool(b[0] & 0x08)
        qos = (b[0] >> 1) & 0x3
        retain = bool(b[0] & 0x01)
        length = await self._receive_length()
        if length > 0:
            payload = await self._reader.read(length)
            if len(payload) != length:
                raise MQTTConnectionInterrupted
        else:
            payload = None
        self._last_received_message_timestamp = time.time()
        return type_, dup, qos, retain, payload

    async def _message_receiver(self):
        while True:
            try:
                try:
                    type_, dup, qos, retain, payload = await self._receive_message()
                except MQTTConnectionInterrupted:
                    for message_id, response_event in self._response_events.items():
                        response_event.set()
                    break
                if type_ in (4, 9, 11):  # PUBACK, SUBACK, UNSUBACK
                    message_id = int.from_bytes(payload[:2], 'big')
                    # TODO check if message_id exists?
                    self._responses[message_id] = type_, dup, qos, retain, payload[2:] if len(payload) > 2 else None
                    self._response_events[message_id].set()
                elif type_ == 3:  # PUBLISH
                    topic_length = int.from_bytes(payload[:2], 'big')
                    topic = payload[2:topic_length + 2].decode()
                    if qos > 0:
                        message_id = payload[2 + topic_length:2 + topic_length + 2]
                        message_payload = payload[2 + topic_length + 2:]
                        await self._send_message(4, message_id)
                    else:
                        message_payload = payload[2 + topic_length:]
                    self._queue.put(MQTTMessage(topic, message_payload, qos, retain))
                elif type_ == 13:  # PING
                    self._last_ping_response_message_timestamp = time.time()
                else:
                    raise NotImplementedError(f'Unknown message type {type_} (payload: {payload.hex().upper()})')
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f'Unhandled exception in message receiver: {type(e)} ({e})')

    async def _connection_supervisor(self):
        first_connect = True
        while True:
            try:
                if self._writer is None:
                    # TODO add exponential backoff?
                    try:
                        await self._connect(first_connect)
                    except MQTTConnectionError:
                        await asyncio.sleep(5.0)
                else:
                    first_connect = False
                    if time.time() - self._last_sent_message_timestamp > self._keep_alive:
                        self._last_ping_response_message_timestamp = None
                        await self._send_message(12)
                        await asyncio.sleep(self._keep_alive)
                        if self._last_ping_response_message_timestamp is None:
                            await self._disconnect()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f'Unhandled exception in connection supervisor: {type(e)} ({e})')

    async def _wait_for_response(self, message_id: int) -> tuple[int, bool, int, bool, bytes | None]:
        await self._response_events[message_id].wait()
        del self._response_events[message_id]
        response = self._responses.pop(message_id, None)
        if response is None:
            raise MQTTConnectionInterrupted
        return response
