import inspect
import math

import markdown2

import backtrader as bt

from backtrader_plotting.utils import get_params, paramval2str
from backtrader_plotting.bokeh import labelizer


def _get_parameter_table(params) -> str:
    params = get_params(params)

    if len(params) == 0:
        params[''] = ''
    else:
        for k, v in params.items():
            params[k] = paramval2str(k, v)

    return _get_table(['Parameter', 'Value'], params)


def _get_table(header, data) -> str:
    table = f'|{header[0]}|{header[1]}|\n'
    table += f'|--|--|\n'
    for k, v in data.items():
        table += f'|{k}|{v}|\n'
    return table


def _get_datas(strategy: bt.Strategy) -> str:
    md = '\n# Data Feeds\n'

    for data in strategy.datas:
        md += f'## {data.__class__.__name__}\n'

        tabdata = {
            'DataName:': str(data._dataname).replace("|", "\\|"),
            'Timezone:': data._tz,
            'Number of bars:': len(data),
            'Bar Length:': f"{data._compression} {bt.TimeFrame.getname(data._timeframe, data._compression)}",
        }

        # live trading does not have valid data parameters (other datas might also not have)
        if not math.isinf(data.fromdate):
            tabdata['Time From:'] = bt.num2date(data.fromdate)

        if not math.isinf(data.todate):
            tabdata['Time To:'] = bt.num2date(data.todate)

        md += _get_table(['Property', 'Value'], tabdata)

    return md


def _get_strategy(strategy: bt.Strategy, include_src=True) -> str:
    md = f'\n# Strategy: {strategy.__class__.__name__}\n'

    md += _get_parameter_table(strategy.params)

    md += '## Indicators:\n\n'
    for i in strategy.getindicators():
        md += f'### {labelizer.label(i)}\n\n'
        md += _get_parameter_table(i.params)

    if include_src:
        md += 'Source Code:\n'
        md += f'\n```\n{inspect.getsource(strategy.__class__)}\n```\n\n'

    return md


def _get_analyzers(strategy: bt.Strategy) -> str:
    if len(strategy.analyzers) == 0:
        return ""

    md = '\n# Analyzers\n'

    for a in strategy.analyzers:
        md += f'## {a.__class__.__name__}\n'
        md += _get_parameter_table(a.params)

    return md


def _get_observers(strategy: bt.Strategy) -> str:
    md = '\n# Observers\n'

    for o in strategy.observers:
        md += f'## {o.__class__.__name__}\n'
        md += _get_parameter_table(o.params)

    return md


def get_metadata_div(strategy: bt.Strategy, include_src=True) -> str:
    md = ""

    md += _get_strategy(strategy, include_src)
    md += '* * *'
    md += _get_datas(strategy)
    md += '* * *'
    md += _get_observers(strategy)
    md += '* * *'
    md += _get_analyzers(strategy)
    md += '* * *'

    css_classes = {'table': 'metaDataTable'}

    html = markdown2.markdown(md, extras={
        'fenced-code-blocks': None,
        'tables': None,
        'html-classes': css_classes
    })
    return html
