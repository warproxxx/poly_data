from collections import defaultdict
import functools
from typing import Dict, Callable, Tuple

from pandas import DataFrame

from bokeh.models import ColumnDataSource, Model
from bokeh.layouts import column
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter, StringFormatter

from backtrader_plotting import Bokeh
from backtrader_plotting.bokeh.bokeh_webapp import BokehWebapp


class OptBrowser:
    def __init__(self, bokeh: Bokeh, optresults, usercolumns: Dict[str, Callable] = None, num_result_limit=None, sortcolumn=None, sortasc=True):
        self._usercolumns = {} if usercolumns is None else usercolumns
        self._num_result_limit = num_result_limit
        self._bokeh: Bokeh = bokeh
        self._sortcolumn = sortcolumn
        self._sortasc = sortasc
        self._optresults = optresults

    def start(self, ioloop=None):
        webapp = BokehWebapp("Backtrader Optimization Result", "basic.html.j2", self._bokeh.params.scheme, self.build_optresult_model)
        webapp.start(ioloop)

    def _build_optresult_selector(self, optresults) -> Tuple[DataTable, ColumnDataSource]:
        # 1. build a dict with all params and all user columns
        data_dict = defaultdict(list)
        for optres in optresults:
            for param_name, _ in optres[0].params._getitems():
                param_val = optres[0].params._get(param_name)
                data_dict[param_name].append(param_val)

            for usercol_label, usercol_fnc in self._usercolumns.items():
                data_dict[usercol_label].append(usercol_fnc(optres))

        # 2. build a pandas DataFrame
        df = DataFrame(data_dict)

        # 3. now sort and limit result
        if self._sortcolumn is not None:
            df = df.sort_values(by=[self._sortcolumn], ascending=self._sortasc)

        if self._num_result_limit is not None:
            df = df.head(self._num_result_limit)

        # 4. build column info for Bokeh table
        tab_columns = []
        for colname in data_dict.keys():
            formatter = NumberFormatter(format='0.000')

            if len(data_dict[colname]) > 0 and isinstance(data_dict[colname][0], int):
                formatter = StringFormatter()

            tab_columns.append(TableColumn(field=colname, title=f'{colname}', sortable=False, formatter=formatter))

        # TODO: currently table size is hardcoded
        cds = ColumnDataSource(df)
        selector = DataTable(source=cds, columns=tab_columns, width=1600, height=150)
        return selector, cds

    def build_optresult_model(self, _=None) -> Model:
        """Generates and returns an interactive model for an OptResult or an OrderedOptResult"""

        # we have list of results, each result contains the result for one strategy. we don't support having more than one strategy!
        if len(self._optresults) > 0 and len(self._optresults[0]) > 1:
            raise RuntimeError("You passed on optimization result based on more than one strategy which is not supported!")

        selector, selector_cds = self._build_optresult_selector(self._optresults)

        def _get_model(idx: int):
            return self._bokeh.plot_and_generate_optmodel(self._optresults[idx][0])

        #  first zero is because we show the first opt result by default and second zero cause we support only 1 strategy
        model = column([selector, _get_model(0)])
        model.background = self._bokeh.params.scheme.background_fill

        def update(_name, _old, new):
            if len(new) == 0:
                return

            stratidx = new[0]
            model.children[-1] = _get_model(stratidx)

        selector_cds.selected.on_change('indices', update)

        return model
