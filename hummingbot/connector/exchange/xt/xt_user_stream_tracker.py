#!/usr/bin/env python

import asyncio
from typing import (
    Optional,
    List,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.data_type.user_stream_tracker import (
    UserStreamTracker
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.xt.xt_api_user_stream_data_source import \
    XtAPIUserStreamDataSource
from hummingbot.connector.exchange.xt.xt_auth import XtAuth
from hummingbot.connector.exchange.xt.xt_constants import EXCHANGE_NAME
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class XtUserStreamTracker(UserStreamTracker):

    def __init__(self,
                 throttler: AsyncThrottler,
                 xt_auth: Optional[XtAuth] = None,
                 trading_pairs: Optional[List[str]] = []):
        super().__init__()
        self._xt_auth: XtAuth = xt_auth
        self._trading_pairs: List[str] = trading_pairs
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None
        self._throttler = throttler

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        """
        *required
        Initializes a user stream data source (user specific order diffs from live socket stream)
        :return: OrderBookTrackerDataSource
        """
        if not self._data_source:
            self._data_source = XtAPIUserStreamDataSource(
                throttler=self._throttler,
                xt_auth=self._xt_auth,
                trading_pairs=self._trading_pairs
            )
        return self._data_source

    @property
    def exchange_name(self) -> str:
        """
        *required
        Name of the current exchange
        """
        return EXCHANGE_NAME

    async def start(self):
        """
        *required
        Start all listeners and tasks
        """
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
