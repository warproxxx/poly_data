import asyncio
from collections import defaultdict
from enum import Enum
from datetime import datetime
import logging
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple
import threading

import numpy as np

import backtrader as bt

from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.document import Document

from backtrader_plotting.bokeh.bokeh_webapp import BokehWebapp
from backtrader_plotting.schemes import Blackly
from backtrader_plotting import Bokeh
from backtrader_plotting.bokeh.live.liveclient import LiveClient

import pandas

import tornado.ioloop

_logger = logging.getLogger(__name__)


class _PatchPackage:
    def __init__(self, column_name: str, dt: datetime, value):
        self.column_name: str = column_name
        self.value = value
        self.datetime: datetime = dt  # if this is None then it will update the last data


class PlotListener(bt.ListenerBase):
    params = (
        ('scheme', Blackly()),
        ('style', 'bar'),
        ('lookback', 23),
        ('strategyidx', 0),
        ('http_port', 80),
        ('title', 'Live'),
    )

    class UpdateType(Enum):
        APPEND = 1,
        UPDATE_LAST = 2,
        FILL_OR_PREPEND = 3,

    def __init__(self, **kwargs):
        self._cerebro: Optional[bt.Cerebro] = None
        self._webapp = BokehWebapp(self.p.title,
                                   'basic.html.j2',
                                   self.p.scheme,
                                   self._bokeh_cb_build_root_model,
                                   on_session_destroyed=self._on_session_destroyed,
                                   port=self.p.http_port)
        self._lock = Lock()
        self._datastore = None
        self._clients: Dict[str, LiveClient] = {}
        self._bokeh_kwargs = kwargs
        self._bokeh = self._create_bokeh()
        self._pkgs_insert = defaultdict(lambda: [])
        self._prev_strategy_len = 0
        self._reset_patch_pkgs()

    def _reset_patch_pkgs(self):
        self._patch_pkgs: Dict[str, Dict[str, Any]] = defaultdict(lambda: {})

    def _create_bokeh(self):
        return Bokeh(style=self.p.style, scheme=self.p.scheme, **self._bokeh_kwargs)  # get a copy of the scheme so we can modify it per client

    def _on_session_destroyed(self, session_context):
        with self._lock:
            del self._clients[session_context.id]

    def _bokeh_cb_build_root_model(self, doc: Document):
        client = LiveClient(doc,
                            self._create_bokeh,
                            self._bokeh_cb_push_adds,
                            self._cerebro.runningstrats[self.p.strategyidx],
                            lookback=self.p.lookback)

        with self._lock:
            self._clients[doc.session_context.id] = client

        self._bokeh_cb_push_adds(doc)

        return client.model

    def start(self, cerebro):
        _logger.info("Starting PlotListener...")

        self._cerebro = cerebro

        self._datastore = self._bokeh.build_strategy_data(self._cerebro.runningstrats[self.p.strategyidx])

        t = threading.Thread(target=self._t_bokeh_server)
        t.daemon = True
        t.start()

    def stop(self):
        pass

    def _t_bokeh_server(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = tornado.ioloop.IOLoop.current()
        self._webapp.start(loop)

    def _bokeh_cb_push_adds(self, bootstrap_document=None):
        if bootstrap_document is None:
            document = curdoc()
        else:
            document = bootstrap_document

        with self._lock:
            client: LiveClient = self._clients[document.session_context.id]
            updatepkg_df: pandas.DataFrame = self._datastore[self._datastore['index'] > client.last_index]
            # skip if we don't have new data
            if updatepkg_df.shape[0] == 0:
                return

            updatepkg = ColumnDataSource.from_df(updatepkg_df)
            client.push_adds(updatepkg)

    def _bokeh_full_refresh(self):
        document = curdoc()
        session_id = document.session_context.id
        with self._lock:
            client: LiveClient = self._clients[session_id]
            client.push_full_refresh(self._datastore)

            # remove any pending patch packages as we just have issued a full update
            self._reset_patch_pkgs()

    def _bokeh_cb_push_patches(self):
        document = curdoc()
        session_id = document.session_context.id
        with self._lock:
            client: LiveClient = self._clients[session_id]

            patch_pkgs = self._patch_pkgs[session_id]
            self._patch_pkgs[session_id] = {}  # reset
            _logger.info("Patch pkg: " + str(patch_pkgs))
            client.push_patches(patch_pkgs)

    def _queue_patch_pkg(self, current_frame):
        last_index = self._datastore.index[-1]
        for column_name in current_frame.columns:
            d = current_frame[column_name].iloc[0]
            if isinstance(d, float) and np.isnan(d):
                continue
            self._datastore.at[last_index, column_name] = d  # update data in datastore
            for sess_id in self._clients.keys():
                self._patch_pkgs[sess_id][column_name] = d

                # WIP: make curernt bar outline red
                # if column_name.endswith('outline'):
                #    self._patch_pkgs[sess_id].append((column_name, None, '#ff0000'))

    def _detect_update_type(self, strategy):
        # treat as update of old data if strategy datetime is duplicated and we have already data stored
        # in this case data in an older slot was added
        if len(strategy) == self._prev_strategy_len:
            return self.UpdateType.UPDATE_LAST
        else:
            assert len(strategy) > self._prev_strategy_len
            if len(strategy) == 1 or self._datastore['datetime'].iloc[-1] != bt.num2date(strategy.datetime[0]):
                return self.UpdateType.APPEND
            elif self._datastore['datetime'].iloc[-1] == bt.num2date(strategy.datetime[0]):
                # either data was added to the front or data in between was filled
                return self.UpdateType.FILL_OR_PREPEND
            else:
                raise RuntimeError('Update type detection failed')

    def next(self):
        with self._lock:
            strategy = self._cerebro.runningstrats[self.p.strategyidx]
            update_type = self._detect_update_type(strategy)
            self._prev_strategy_len = len(strategy)

            _logger.info(f"next: update type: {update_type}")
            if update_type == self.UpdateType.UPDATE_LAST:
                startidx = int(self._datastore['index'].iloc[-1])
                current_frame = self._bokeh.build_strategy_data(strategy, num_back=1, startidx=startidx)
                self._queue_patch_pkg(current_frame)
                for client in self._clients.values():
                    client.document.add_next_tick_callback(self._bokeh_cb_push_patches)
            elif update_type == self.UpdateType.FILL_OR_PREPEND:
                self._datastore = self._bokeh.build_strategy_data(strategy)
                for client in self._clients.values():
                    _logger.info('Adding full refersh callback')
                    client.add_fullrefresh_callback(self._bokeh_full_refresh, 500)
            elif update_type == self.UpdateType.APPEND:
                nextidx = 0 if self._datastore.shape[0] == 0 else int(self._datastore['index'].iloc[-1]) + 1

                num_back = 1 if self._datastore.shape[0] > 0 else None  # if we have NO data yet then fetch all (first call)
                new_frame = self._bokeh.build_strategy_data(strategy, num_back=num_back, startidx=nextidx)

                # append data and remove old data
                self._datastore = self._datastore.append(new_frame)
                self._datastore = self._datastore.tail(self.p.lookback)

                for client in self._clients.values():
                    doc = client.document
                    try:
                        doc.remove_next_tick_callback(self._bokeh_cb_push_adds)
                    except ValueError:
                        # there was no callback to remove
                        pass
                    doc.add_next_tick_callback(self._bokeh_cb_push_adds)
            else:
                raise RuntimeError(f'Unexepected update_type: {update_type}')
