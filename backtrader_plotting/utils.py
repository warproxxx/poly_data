import bisect
import datetime
from enum import Enum
import logging
import math
import operator
from typing import Dict, Optional, List, Union

import backtrader as bt

import numpy as np
import pandas as pd
import itertools


_logger = logging.getLogger(__name__)


def paramval2str(name, value):
    if value is None:  # catch None value early here!
        return str(value)
    elif name == "timeframe":
        return bt.TimeFrame.getname(value, 1)
    elif isinstance(value, float):
        return f"{value:.2f}"
    elif isinstance(value, (list,tuple)):
        return ','.join(value)
    elif isinstance(value, type):
        return value.__name__
    else:
        return str(value)


def get_nondefault_params(params: object) -> Dict[str, object]:
    return {key: params._get(key) for key in params._getkeys() if not params.isdefault(key)}


def get_params(params: bt.AutoInfoClass):
    return {key: params._get(key) for key in params._getkeys()}


def get_params_str(params: Optional[bt.AutoInfoClass]) -> str:
    user_params = get_nondefault_params(params)
    plabs = [f"{x}: {paramval2str(x, y)}" for x, y in user_params.items()]
    plabs = '/'.join(plabs)
    return plabs


def nanfilt(x: List) -> List:
    """filters all NaN values from a list"""
    return [value for value in x if not math.isnan(value)]


def convert_to_master_clock(line, line_clk, master_clock, forward_fill=False):
    """Takes a clock and generates an appropriate line with a value for each entry in clock. Values are taken from another line if the
    clock value in question is found in its line_clk. Otherwise NaN is used"""
    if master_clock is None:
        return line

    # sometimes the clock has more data than the data line
    # not sure when this is the case
    # i think both are left aligned so for latest clock values there is no data
    clk_offset = len(line_clk) - len(line)
    new_line = []  # hold the data for the resampled line
    next_start_idx = 0
    prev_lvalue = np.nan  # conserve previous line value for fill_by_prev
    # iterate each master_clock entry and try to find a matching clock in the source line
    for mc in master_clock:
        found = False
        for i in range(next_start_idx, len(line_clk)):
            lc = line_clk[i]
            line_idx = i - clk_offset

            # data line might be shorter so we don't have data
            lvalue = np.nan if line_idx < 0 else line[line_idx]

            if mc == lc:
                new_line.append(lvalue)
                next_start_idx = i  # start searching from *this* index to have a chance to catch it as prev value next round
                found = True
                break
            elif lc > mc:
                # no need to keep searching...
                break
            else:
                prev_lvalue = lvalue

        if not found:
            if forward_fill:
                fill_v = prev_lvalue  # fill missing values with prev value
            else:
                fill_v = float('nan')  # fill with NaN, Bokeh wont plot
            new_line.append(fill_v)
    return new_line


def convert_to_pandas(master_clock, obj: bt.LineSeries, start: datetime.datetime = None, end: datetime.datetime = None, name_prefix: str = "", num_back=None) -> pd.DataFrame:
    lines_clk = obj.lines.datetime.plotrange(start, end)

    df = pd.DataFrame()
    # iterate all lines
    for lineidx in range(obj.size()):
        line = obj.lines[lineidx]
        linealias = obj.lines._getlinealias(lineidx)
        if linealias == 'datetime':
            continue

        # get data limited to time range
        data = line.plotrange(start, end)

        ndata = convert_to_master_clock(data, lines_clk, master_clock)

        df[name_prefix + linealias] = ndata

    df[name_prefix + 'datetime'] = [bt.num2date(x) for x in master_clock]

    return df


def get_clock_line(obj: Union[bt.ObserverBase, bt.IndicatorBase, bt.StrategyBase]):
    """Find the corresponding clock for an object. A clock is a datetime line that holds timestamps for the line in question."""
    if isinstance(obj, (bt.ObserverBase, bt.IndicatorBase, bt.MultiCoupler)):
        return get_clock_line(obj._clock)
    elif isinstance(obj, (bt.StrategyBase, bt.AbstractDataBase)):
        clk = obj
    elif isinstance(obj, bt.LineSeriesStub):
        # indicators can be created to run on a single line (instead of e.g. a data object)
        # in that case we grab the owner of that line to find the corresponding clok
        return get_clock_line(obj._owner)
    elif isinstance(obj, bt.LineActions):
        # used for line actions like "macd > data[0]"
        return get_clock_line(obj._owner)
    else:
        raise Exception(f'Unsupported object type passed: {obj.__class__}')
    return clk.lines.datetime


def build_master_clock(strategy: bt.Strategy,
                       start: Optional[datetime.datetime] = None, end: Optional[datetime.datetime] = None,
                       ):
    """Build the master clock which is a clock line that is basically a merged line of all available clocks"""
    master_clock = []
    for obj in itertools.chain(strategy.datas, strategy.getindicators(), strategy.getobservers()):
        line_clk = get_clock_line(obj).plotrange(start, end)
        master_clock += line_clk

    # remove duplicates
    master_clock = list(dict.fromkeys(master_clock))

    master_clock.sort()

    return master_clock


def get_strategy_start_end(strategy, start, end):
    """Get start and end indices for strategy by given start and end datetimes."""
    st_dtime = strategy.lines.datetime.array
    if start is None:
        start = 0
    if end is None:
        end = len(st_dtime)

    if isinstance(start, datetime.date):
        start = bisect.bisect_left(st_dtime, bt.date2num(start))

    if isinstance(end, datetime.date):
        end = bisect.bisect_right(st_dtime, bt.date2num(end))

    if end < 0:
        end = len(st_dtime) + 1 + end

    return start, end


def find_by_plotid(strategy: bt.Strategy, plotid: str):
    """Finds the object with a give plotid in a strategy's datas, indicators and observers"""
    objs = itertools.chain(strategy.datas, strategy.getindicators(), strategy.getobservers())
    founds = []
    for obj in objs:
        if getattr(obj.plotinfo, 'plotid', None) == plotid:
            founds.append(obj)

    num_results = len(founds)
    if num_results == 0:
        return None
    elif num_results == 1:
        return founds[0]
    else:
        raise RuntimeError(f'Found multiple objects with plotid "{plotid}"')


class PlotType(Enum):
    MARKER = 1,
    LINE = 2,
    BAR = 3,


def get_plottype(obj, lineidx: int) -> PlotType:
    lineplotinfo = get_plotlineinfo(obj, lineidx)
    marker = lineplotinfo._get('marker', None)
    if marker is not None:
        return PlotType.MARKER

    method = lineplotinfo._get('_method', 'line')
    return PlotType.LINE if method == 'line' else PlotType.BAR


def get_indobs_dataobj(indicator: bt.Indicator):
    """The indicator might have been created using a specific line (like SMA(data.lines.close)). In this case
    a LineSeriesStub has been created for which we have to resolve the original data"""
    data = indicator.data
    if isinstance(data, bt.LineSeriesStub):
        return data._owner
    else:
        return data


def get_tradingdomain(obj) -> Union[str, bool]:
    """Returns the trading domain in effect for an object. This either the value of the plotinfo attribute or it will be resolved up chain."""
    td = obj.plotinfo.tradingdomain
    if td is not None:
        return td

    if isinstance(obj, bt.AbstractDataBase):
        # data feeds are end points
        return obj._name
    elif isinstance(obj, bt.IndicatorBase):
        # lets find the data the indicator is based on
        data = get_indobs_dataobj(obj)
        return get_tradingdomain(data)
    elif isinstance(obj, bt.ObserverBase):
        # distinguish between observers related to data and strategy wide observers
        if isinstance(obj._clock, bt.AbstractDataBase):
            return get_tradingdomain(obj._clock)
        else:
            return True  # for strategy wide observers we return True which means it belongs to all logic groups
    else:
        raise Exception('unsupported')


def get_plotlineinfo(obj, lineidx):
    lineplotinfo = None
    if not isinstance(obj.lines, list):
        linealias = obj.lines._getlinealias(lineidx)
        lineplotinfo = getattr(obj.plotlines, '_%d' % lineidx, None)
        if not lineplotinfo and linealias is not None:
            lineplotinfo = getattr(obj.plotlines, linealias, None)

    if not lineplotinfo:
        lineplotinfo = bt.AutoInfoClass()
    return lineplotinfo


def get_source_id(obj) -> str:
    return str(id(obj))


def get_ind_areas(ind, lineidx):
    """Generates indicator area information to support _fill_gt and _fill_lt"""
    line = ind.lines[lineidx]
    source_id = get_source_id(line)
    lineplotinfo = get_plotlineinfo(ind, lineidx)
    alpha = None

    for suffix, comp_op in (('_gt', operator.gt), ('_lt', operator.lt), ('', None),):
        attr_name = '_fill' + suffix
        ref, color = lineplotinfo._get(attr_name, (None, None))

        if ref is None:
            continue

        # fcol can be a tuple/list to also specifyy alpha
        if isinstance(color, (list, tuple)):
            color, alpha = color

        if isinstance(ref, int):
            y2 = ref  # static value
        elif isinstance(ref, str):
            # ref to another line
            l2 = getattr(ind.lines, ref)
            y2 = get_source_id(l2)
        else:
            raise RuntimeError('Unsupported fref')

        if comp_op is not None:
            # we need to build a custom data line applying the operator
            y1 = source_id + attr_name
        else:
            # we can use the original data as is
            y1 = source_id

        yield attr_name, y1, y2, color, alpha, comp_op


def get_lines(obj):
    """Generates all lines of an object yielding their indices, line object and the corresponding source id"""
    num_lines = obj.size() if getattr(obj, 'size', None) else 1
    for lineidx in range(num_lines):
        line = obj.lines[lineidx]
        source_id = get_source_id(line)
        yield lineidx, line, source_id
