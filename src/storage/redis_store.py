"""
Redis storage for features and events
"""

import json
from datetime import datetime, timedelta
from typing import Any, Optional
import logging

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RedisStore:
    """
    Redis storage for feature cache

    Proactive refresh every 1 minute
    TTL: 120 minutes for features
    """

    FEATURE_TTL = 7200  # 120 minutes
    REFRESH_INTERVAL = 60  # 1 minute

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        max_connections: int = 50,
    ):
        self.host = host
        self.port = port
        self.password = password
        self.db = db

        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            max_connections=max_connections,
            decode_responses=True,
        )

        self.client = redis.Redis(connection_pool=self.pool)

        logger.info(f"Initialized RedisStore at {host}:{port}")

    async def store_features(
        self,
        pod: str,
        namespace: str,
        features: dict[str, Any],
        timestamp: datetime,
    ):
        """
        Store features for a pod

        Args:
            pod: Pod name
            namespace: Namespace
            features: Dict of feature name -> value
            timestamp: Feature timestamp
        """
        for feature_name, value in features.items():
            key = f"features:{namespace}:{pod}:{feature_name}"

            # Store as JSON with timestamp
            data = {
                "value": value,
                "timestamp": timestamp.isoformat(),
            }

            await self.client.setex(
                key,
                self.FEATURE_TTL,
                json.dumps(data),
            )

    async def get_features(
        self,
        pod: str,
        namespace: str,
        window_minutes: int = 90,
    ) -> dict[str, list[tuple[datetime, Any]]]:
        """
        Get features for a pod (historical window)

        Note: For time-series data, consider using RedisTimeSeries module
        This implementation uses sorted sets for history

        Args:
            pod: Pod name
            namespace: Namespace
            window_minutes: Historical window

        Returns:
            Dict of feature_name -> list of (timestamp, value) tuples
        """
        features = {}

        # Get all feature keys for this pod
        pattern = f"features:{namespace}:{pod}:*"
        async for key in self.client.scan_iter(match=pattern):
            # Extract feature name
            feature_name = key.split(":")[-1]

            # Get current value
            data = await self.client.get(key)
            if data:
                data_dict = json.loads(data)
                value = data_dict["value"]
                ts = datetime.fromisoformat(data_dict["timestamp"])

                if feature_name not in features:
                    features[feature_name] = []
                features[feature_name].append((ts, value))

        return features

    async def get_feature_current(
        self,
        pod: str,
        namespace: str,
        feature_name: str,
    ) -> Optional[Any]:
        """
        Get current value of a feature

        Args:
            pod: Pod name
            namespace: Namespace
            feature_name: Feature name

        Returns:
            Current feature value or None
        """
        key = f"features:{namespace}:{pod}:{feature_name}"
        data = await self.client.get(key)

        if data:
            data_dict = json.loads(data)
            return data_dict["value"]
        return None

    async def features_are_fresh(
        self,
        pod: str,
        namespace: str,
        max_age_seconds: int = 60,
    ) -> bool:
        """
        Check if features were updated within max_age_seconds
        """
        key = f"features:{namespace}:{pod}:cpu_usage_rate"
        ttl = await self.client.ttl(key)

        if ttl == -1:  # No TTL set
            return False

        return ttl > (self.FEATURE_TTL - max_age_seconds)

    async def close(self):
        """Close Redis connection"""
        await self.client.close()
        await self.pool.disconnect()

    async def write_stream_message(
        self,
        stream_name: str,
        payload: dict[str, Any],
    ) -> str:
        """Write a durable pub/sub message using Redis Streams."""
        return await self.client.xadd(stream_name, payload)

    async def read_stream_messages(
        self,
        stream_name: str,
        last_id: str = "$",
        block_ms: int = 1000,
        count: int = 10,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read new messages from a Redis Stream."""
        import redis.exceptions
        try:
            messages = await self.client.xread(
                {stream_name: last_id},
                block=block_ms,
                count=count,
            )
        except (redis.exceptions.TimeoutError, TimeoutError):
            return []
        except (redis.exceptions.ConnectionError, ConnectionError) as e:
            logger.warning(f"Redis connection issue during xread on {stream_name}: {e}")
            return []

        out: list[tuple[str, dict[str, Any]]] = []
        for _, items in messages:
            for msg_id, fields in items:
                out.append((msg_id, fields))
        return out


class EventStore:
    """
    Redis storage for K8s events

    Dual-storage strategy:
    - event:last:{pod}:{type} - Fast lookup (single timestamp)
    - event:history:{pod}:{type} - Bounded history (sorted set)

    TTL: 48-72 hours
    """

    EVENT_TTL = 259200  # 72 hours (3 days)
    HISTORY_MAX_SIZE = 100  # Last N events

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        logger.info("Initialized EventStore")

    async def record_event(
        self,
        pod_name: str,
        event_type: str,
        timestamp: datetime,
    ):
        """
        Store event in both fast lookup and history

        Args:
            pod_name: Pod name
            event_type: Event type (restart, scaling, eviction, etc.)
            timestamp: Event timestamp
        """
        # Fast lookup (single key)
        last_key = f"event:last:{pod_name}:{event_type}"
        await self.redis.setex(last_key, self.EVENT_TTL, timestamp.isoformat())

        # Bounded history (sorted set with timestamp as score)
        history_key = f"event:history:{pod_name}:{event_type}"
        await self.redis.zadd(
            history_key,
            {timestamp.isoformat(): timestamp.timestamp()},
        )

        # Trim to last N events
        await self.redis.zremrangebyrank(
            history_key,
            0,
            -self.HISTORY_MAX_SIZE - 1,
        )

        # Set TTL on history
        await self.redis.expire(history_key, self.EVENT_TTL)

        logger.debug(
            f"Recorded event: pod={pod_name}, type={event_type}, "
            f"time={timestamp.isoformat()}"
        )

    async def get_time_since_last_event(
        self,
        pod_name: str,
        event_type: str,
    ) -> float:
        """
        Get time since last event (O(1) lookup)

        Args:
            pod_name: Pod name
            event_type: Event type

        Returns:
            Seconds since last event, or inf if no event
        """
        last_key = f"event:last:{pod_name}:{event_type}"
        last_timestamp_str = await self.redis.get(last_key)

        if last_timestamp_str:
            last_timestamp = datetime.fromisoformat(last_timestamp_str)
            return (datetime.now() - last_timestamp).total_seconds()

        return float("inf")

    async def get_event_history(
        self,
        pod_name: str,
        event_type: str,
        last_n: int = 10,
    ) -> list[datetime]:
        """
        Get last N event timestamps

        Args:
            pod_name: Pod name
            event_type: Event type
            last_n: Number of events to retrieve

        Returns:
            List of event timestamps (most recent first)
        """
        history_key = f"event:history:{pod_name}:{event_type}"

        # Get last N from sorted set (highest scores = most recent)
        events = await self.redis.zrevrange(
            history_key,
            0,
            last_n - 1,
        )

        return [datetime.fromisoformat(e) for e in events]

    async def get_event_count(
        self,
        pod_name: str,
        event_type: str,
        window_hours: int = 24,
    ) -> int:
        """
        Count events in time window

        Args:
            pod_name: Pod name
            event_type: Event type
            window_hours: Time window in hours

        Returns:
            Event count
        """
        history_key = f"event:history:{pod_name}:{event_type}"

        # Get timestamp range
        now = datetime.now()
        start = now - timedelta(hours=window_hours)

        # Count events in range
        count = await self.redis.zcount(
            history_key,
            start.timestamp(),
            now.timestamp(),
        )

        return count
