import bisect
import datetime
import itertools
import logging
import re
import os
import sys
import tempfile
from typing import List, Dict, Optional, Union, Tuple

import backtrader as bt

import numpy as np

import pandas as pd

from bokeh.models import ColumnDataSource, Model
from bokeh.models.widgets import Panel, Tabs
from bokeh.layouts import column, gridplot

from bokeh.embed import file_html
from bokeh.models.widgets import Div
from bokeh.resources import CDN
from bokeh.util.browser import view

from jinja2 import Environment, PackageLoader

from backtrader_plotting.bokeh.utils import generate_stylesheet, append_cds
from backtrader_plotting.utils import convert_to_master_clock, get_clock_line, find_by_plotid, convert_to_pandas, get_indobs_dataobj, get_tradingdomain, get_plottype, PlotType, get_plotlineinfo, get_source_id, get_ind_areas, get_lines, build_master_clock, get_strategy_start_end
from backtrader_plotting.bokeh.figure import Figure, HoverContainer
from backtrader_plotting.bokeh.datatable import TableGenerator
from backtrader_plotting.bokeh import labelizer
from backtrader_plotting.schemes import Blackly
from backtrader_plotting.schemes.scheme import Scheme
from backtrader_plotting.html import metadata
import operator

_logger = logging.getLogger(__name__)


if 'ipykernel' in sys.modules:
    from IPython.core.display import display, HTML
    from bokeh.io import output_notebook, show
    output_notebook()


class FigurePage(object):
    def __init__(self, obj: Union[bt.Strategy, bt.OptReturn]):
        self.figures: List[Figure] = []
        self.strategy: Optional[bt.Strategy] = obj if isinstance(obj, bt.Strategy) else None
        self.cds: Optional[ColumnDataSource] = ColumnDataSource(data=dict(datetime=np.array([], dtype=np.datetime64), index=np.array([], np.float64)))
        self.analyzers: List[bt.Analyzer, bt.MetaStrategy, Optional[bt.AutoInfoClass]] = []
        self.model: Optional[Model] = None  # the whole generated model will we attached here after plotting

    def get_tradingdomains(self) -> List[str]:
        """Return a list of all aggregated tradingdomains of all FigureEnvs."""
        tradingdomain = set()
        for fe in self.figures:
            tradingdomain = tradingdomain.union(fe.get_tradingdomains())
        return list(tradingdomain)


class Bokeh(metaclass=bt.MetaParams):
    params = (('scheme', Blackly()),
              ('filename', None),
              ('plotconfig', None),
              ('output_mode', 'show'),
              ('show', True)
              )

    def __init__(self, **kwargs):
        for pname, pvalue in kwargs.items():
            setattr(self.p.scheme, pname, pvalue)

        self._iplot: Optional[bool] = None
        self._tablegen = TableGenerator(self.p.scheme)
        if not isinstance(self.p.scheme, Scheme):
            raise Exception("Provided scheme has to be a subclass of backtrader_plotting.schemes.scheme.Scheme")

        self._initialized: bool = False
        self._is_optreturn: bool = False  # when optreturn is active during optimization then we get a thinned out result only
        self._current_fig_idx: Optional[int] = None
        self.figurepages: List[FigurePage] = []

    def _configure_plotting(self, strategy: bt.Strategy):
        datas, inds, obs = strategy.datas, strategy.getindicators(), strategy.getobservers()

        for objs in [datas, inds, obs]:
            for idx, obj in enumerate(objs):
                self._configure_plotobject(obj, idx, strategy)

    def _configure_plotobject(self, obj, idx, strategy):
        if self.p.plotconfig is None:
            return

        def apply_config(obj, config):
            for k, v in config.items():
                if k == 'plotmaster':
                    # this needs special treatment since a string is passed but we need to set the actual obj
                    v = find_by_plotid(strategy, v)

                setattr(obj.plotinfo, k, v)

        for k, config in self.p.plotconfig.items():
            ctype, target = k.split(':')

            if ctype == 'r':  # regex
                label = labelizer.label(obj)

                m = re.match(target, label)
                if m:
                    apply_config(obj, config)
            elif ctype[0] == '#':  # index
                target_type, target_idx = target.split('-')

                target_types = {
                    'i': bt.Indicator,
                    'o': bt.Observer,
                    'd': bt.AbstractDataBase,
                }

                # check if instance type matches
                if not isinstance(obj, target_types[target_type]):
                    continue

                if target_idx != '*' and int(target_idx) != idx:
                    continue
                apply_config(obj, config)
            elif ctype == 'id':  # plotid
                plotid = getattr(obj.plotinfo, 'plotid', None)
                if plotid is None or plotid != target:
                    continue
                apply_config(obj, config)
            else:
                raise RuntimeError(f'Unknown config type in plotting config: {k}')

    def list_tradingdomains(self, strategy: bt.Strategy):
        """Return a list of all trading domains to be found in a strategy."""
        data_graph, volume_graph = self._build_graph(strategy.datas, strategy.getindicators(), strategy.getobservers())

        lgs = list()
        for master in itertools.chain(data_graph.keys(), volume_graph):
            lg = get_tradingdomain(master)
            if isinstance(lg, str) and lg not in lgs:
                lgs.append(lg)

        return lgs

    def _build_graph(self, datas, inds, obs, tradingdomain=None) -> Tuple[Dict, List]:
        data_graph = {}
        volume_graph = []
        for d in datas:
            if not d.plotinfo.plot or not Figure.should_filter_by_tradingdomain(d, tradingdomain):
                continue

            pmaster = Bokeh._resolve_plotmaster(d.plotinfo.plotmaster)
            if pmaster is None:
                data_graph[d] = []
            else:
                if pmaster not in data_graph:
                    data_graph[pmaster] = []
                data_graph[pmaster].append(d)

            if self.p.scheme.volume and self.p.scheme.voloverlay is False:
                volume_graph.append(d)

        for obj in itertools.chain(inds, obs):
            if not hasattr(obj, 'plotinfo'):
                # no plotting support cause no plotinfo attribute available - so far LineSingle derived classes
                continue

            # should this indicator be plotted?
            if not obj.plotinfo.plot or obj.plotinfo.plotskip or not Figure.should_filter_by_tradingdomain(obj, tradingdomain):
                continue

            # subplot = create a new figure for this indicator
            subplot: bool = obj.plotinfo.subplot
            plotmaster = obj.plotinfo.plotmaster
            if subplot and plotmaster is None:
                data_graph[obj] = []
            else:
                plotmaster = plotmaster if plotmaster is not None else get_indobs_dataobj(obj)
                plotmaster = Bokeh._resolve_plotmaster(plotmaster)  # resolve possible further plotmasters

                # check if the plostmaster is about to be plotted. otherwise we would activae it here implicitly
                if plotmaster.plotinfo.plot:
                    if plotmaster not in data_graph:
                        data_graph[plotmaster] = []
                    data_graph[plotmaster].append(obj)

        return data_graph, volume_graph

    @property
    def _cur_figurepage(self) -> FigurePage:
        return self.figurepages[self._current_fig_idx]

    @staticmethod
    def _resolve_plotmaster(obj):
        if obj is None:
            return None

        while True:
            pm = obj.plotinfo.plotmaster
            if pm is None:
                break
            else:
                obj = pm
        return obj

    def _blueprint_strategy(self, strategy: bt.Strategy, start=None, end=None, tradingdomain=None, **kwargs) -> None:
        if not strategy.datas:
            return

        self._cur_figurepage.analyzers += [a for _, a in strategy.analyzers.getitems()]

        data_graph, volume_graph = self._build_graph(strategy.datas, strategy.getindicators(), strategy.getobservers(), tradingdomain)

        start, end = get_strategy_start_end(strategy, start, end)

        # reset hover container to not mix hovers with other strategies
        hoverc = HoverContainer(hover_tooltip_config=self.p.scheme.hover_tooltip_config, is_multidata=len(strategy.datas) > 1)

        figure_envs: List[Figure] = []
        for master, slaves in data_graph.items():
            plotorder = getattr(master.plotinfo, 'plotorder', 0)
            figureenv = Figure(strategy, self._cur_figurepage.cds, hoverc, start, end, self.p.scheme, master, plotorder)

            figureenv.plot(master)

            for s in slaves:
                figureenv.plot(s)
            figure_envs.append(figureenv)

        for f in figure_envs:
            f.bfigure.legend.click_policy = self.p.scheme.legend_click
            f.bfigure.legend.location = self.p.scheme.legend_location
            f.bfigure.legend.background_fill_color = self.p.scheme.legend_background_color
            f.bfigure.legend.label_text_color = self.p.scheme.legend_text_color
            f.bfigure.legend.orientation = self.p.scheme.legend_orientation
            # Hide legend if configured in scheme
            if hasattr(self.p.scheme, 'legend_visible'):
                f.bfigure.legend.visible = self.p.scheme.legend_visible

        # link axis
        for i in range(1, len(figure_envs)):
            figure_envs[i].bfigure.x_range = figure_envs[0].bfigure.x_range

        hoverc.apply_hovertips(figure_envs)

        self._cur_figurepage.figures += figure_envs

        # volume graphs
        for v in volume_graph:
            plotorder = getattr(v.plotinfo, 'plotorder', 0)
            figureenv = Figure(strategy, self._cur_figurepage.cds, hoverc, start, end, self.p.scheme, v, plotorder)
            figureenv.plot_volume(v)
            self._cur_figurepage.figures.append(figureenv)

    def plot_and_generate_optmodel(self, obj: Union[bt.Strategy, bt.OptReturn]):
        self._reset()
        self.plot(obj)

        # we support only one strategy at a time so pass fixed zero index
        # if we ran optresults=False then we have a full strategy object -> pass it to get full plot
        return self.generate_model(0)

    @staticmethod
    def _sort_plotobjects(objs: List[Figure]) -> None:
        objs.sort(key=lambda x: x.plotorder)

    def get_figurepage(self, idx: int = 0):
        return self.figurepages[idx]

    def _get_nodata_panel(self):
        chart_grid = gridplot([], toolbar_location=self.p.scheme.toolbar_location, toolbar_options={'logo': None})
        return Panel(child=chart_grid, title="No Data")

    @property
    def is_tabs_single(self) -> bool:
        if self.p.scheme.tabs == 'single':
            return True
        elif self.p.scheme.tabs == 'multi':
            return False
        else:
            raise RuntimeError(f'Invalid tabs parameter "{self.p.scheme.tabs}"')

    def _on_post_generate_tab(self, tab_name: str, figureenvs: List[Figure]):
        """Configure figures after tabs have been assigned"""
        # configure xaxis visibility
        if self.p.scheme.xaxis_pos == "bottom":
            # only show xaxis for last figure
            for i, f in enumerate(figureenvs):
                f.bfigure.xaxis.visible = False if i < len(figureenvs) - 1 else True
        elif self.p.scheme.xaxis_pos == "all":
            # just show xaxis for all figures
            pass
        else:
            raise RuntimeError(f'Unpexted value for xaxis_pos: "{self.p.scheme.xaxis_pos}"')

    # region Generator Methods
    def generate_model_panels(self, fp: FigurePage, tradingdomain=None) -> List[Panel]:
        observers = [x for x in fp.figures if isinstance(x.master, bt.Observer)]
        datas = [x for x in fp.figures if isinstance(x.master, bt.AbstractDataBase)]
        inds = [x for x in fp.figures if isinstance(x.master, bt.Indicator)]

        # now assign figures to tabs
        # 1. assign default tabs if no manual tab is assigned
        for figureenv in [x for x in datas if x.plottab is None]:
            figureenv.plottab = 'Plots' if self.is_tabs_single else 'Datas'

        for figureenv in [x for x in inds if x.plottab is None]:
            figureenv.plottab = 'Plots' if self.is_tabs_single else 'Indicators'

        for figureenv in [x for x in observers if x.plottab is None]:
            figureenv.plottab = 'Plots' if self.is_tabs_single else 'Observers'

        # 2. group panels by desired tabs
        # groupby expects the groups to be sorted or else will produce duplicated groups
        sorted_figs = list(itertools.chain(datas, inds, observers))

        # 3. filter tradingdomains
        if tradingdomain is not None:
            filtered = []
            for f in sorted_figs:
                lgs = f.get_tradingdomains()
                for lg in lgs:
                    if lg is True or lg == tradingdomain:
                        filtered.append(f)
            sorted_figs = filtered

        sorted_figs.sort(key=lambda x: x.plottab)
        tabgroups = itertools.groupby(sorted_figs, lambda x: x.plottab)

        panels = []

        def build_panel(objects, panel_title):
            if len(objects) == 0:
                return

            Bokeh._sort_plotobjects(objects)

            g = gridplot([[x.bfigure] for x in objects],
                         toolbar_options={'logo': None},
                         toolbar_location=self.p.scheme.toolbar_location,
                         sizing_mode=self.p.scheme.plot_sizing_mode,
                         )
            panels.append(Panel(title=panel_title, child=g))
            self._on_post_generate_tab(panel_title, objects)

        for tabname, figures in tabgroups:
            build_panel(list(figures), tabname)
        return panels

    def generate_model(self, figurepage_idx: int = 0) -> Model:
        """Returns a model generated from internal blueprints"""
        if figurepage_idx >= len(self.figurepages):
            raise RuntimeError(f'Cannot generate model for FigurePage with index {figurepage_idx} as there are only {len(self.figurepages)}.')

        figurepage = self.figurepages[figurepage_idx]
        if not self._is_optreturn:
            panels = self.generate_model_panels(figurepage)
        else:
            panels = []

        # now append analyzer tab(s)
        analyzers = figurepage.analyzers
        panel_analyzer = self.get_analyzer_panel(analyzers)
        if panel_analyzer is not None:
            panels.append(panel_analyzer)

        # append meta tab
        if not self._is_optreturn:
            assert figurepage.strategy is not None
            meta = Div(text=metadata.get_metadata_div(figurepage.strategy, self.p.scheme.strategysrc))
            metapanel = Panel(child=meta, title="Meta")
            panels.append(metapanel)

        model = Tabs(tabs=panels)

        # attach the model to the underlying figure for later reference (e.g. unit test)
        figurepage.model = model

        return model
    # endregion

    def get_analyzer_panel(self, analyzers: List[bt.Analyzer]) -> Optional[Panel]:
        if len(analyzers) == 0:
            return None

        acolumns = []
        for analyzer in analyzers:
            table_header, elements = self._tablegen.get_analyzers_tables(analyzer)

            acolumns.append(column([table_header] + elements, sizing_mode='stretch_width'))

        root = gridplot(acolumns, ncols=self.p.scheme.analyzer_tab_num_cols, toolbar_options={'logo': None}, sizing_mode='stretch_width')
        return Panel(child=root, title='Analyzers')

    def _output_stylesheet(self, template="basic.css.j2"):
        return generate_stylesheet(self.p.scheme, template)

    def _output_plot_file(self, model, idx, filename=None, template="basic.html.j2"):
        if filename is None:
            tmpdir = tempfile.gettempdir()
            filename = os.path.join(tmpdir, f"bt_bokeh_plot_{idx}.html")

        env = Environment(loader=PackageLoader('backtrader_plotting.bokeh', 'templates'))
        templ = env.get_template(template)
        templ.globals['now'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html = file_html(model,
                         template=templ,
                         resources=CDN,
                         template_variables=dict(
                             stylesheet=self._output_stylesheet(),
                             show_headline=self.p.scheme.show_headline,
                             )
                         )

        with open(filename, 'w') as f:
            f.write(html)

        return filename

    def savefig(self, fig, filename, width, height, dpi, tight):
        self._generate_output(fig, filename)

    def build_strategy_data(self, strategy: bt.Strategy,
                            start: Optional[datetime.datetime] = None, end: Optional[datetime.datetime] = None,
                            num_back: Optional[int] = None,
                            startidx: int = 0):
        """startidx: index number to write into the dataframe for the index column"""
        strategydf = pd.DataFrame()

        master_clock = build_master_clock(strategy, start, end)

        start, end = get_strategy_start_end(strategy, start, end)

        if num_back is None:
            num_back = len(master_clock)

        master_clock = master_clock[-num_back:]
        strategydf['master_clock'] = master_clock

        # we use timezone of first data. we might see duplicated timestamps here
        dtline = [bt.num2date(x, strategy.datas[0]._tz) for x in master_clock]

        # add an index line to use as x-axis (instead of datetime axis) to avoid datetime gaps (e.g. weekends)
        indices = list(range(startidx, startidx + len(dtline)))
        strategydf['index'] = indices

        strategydf['datetime'] = dtline

        for data in strategy.datas:
            source_id = get_source_id(data)
            df_data = convert_to_pandas(master_clock, data, start, end, source_id)

            strategydf = strategydf.join(df_data)

            df_colors = Figure.build_color_lines(df_data, self.p.scheme, col_open=source_id + 'open', col_close=source_id + 'close', col_prefix=source_id)
            strategydf = strategydf.join(df_colors)

        for obj in itertools.chain(strategy.getindicators(), strategy.getobservers()):
            for lineidx, line, source_id in get_lines(obj):
                dataline = line.plotrange(start, end)

                plottype = get_plottype(obj, lineidx)

                line_clk = get_clock_line(obj).plotrange(start, end)
                dataline = convert_to_master_clock(dataline, line_clk, master_clock, forward_fill=plottype == PlotType.LINE)
                strategydf[source_id] = dataline

        # now iterate again over indicators to calculate area plots (_fill_gt / _fill_lt)
        for ind in strategy.getindicators():
            for lineidx, line, source_id in get_lines(ind):
                for fattr, _, y2, _, _, fop in get_ind_areas(ind, lineidx):
                    if fop is None:
                        continue  # we only need to take care when operator is used

                    if isinstance(y2, int):
                        # scalar value
                        pass
                    elif isinstance(y2, str):
                        y2 = strategydf[y2]
                    else:
                        raise RuntimeError('Unexpected type')

                    dataline_pd = pd.Series(strategydf[source_id])
                    lineid = source_id + fattr
                    strategydf[lineid] = dataline_pd.where(fop(dataline_pd, y2), y2).to_numpy()

        # apply a proper index (should be identical to 'index' column)
        if strategydf.shape[0] > 0:
            strategydf.index = indices
        return strategydf

    #  region interface for backtrader
    def plot(self, obj: Union[bt.Strategy, bt.OptReturn], figid=0, numfigs=1, iplot=True, start=None, end=None, use=None, fill_data=True, tradingdomain=None, **kwargs):
        """Called by backtrader to plot either a strategy or an optimization result."""

        # prepare new FigurePage
        fp = FigurePage(obj)
        self.figurepages.append(fp)
        self._current_fig_idx = len(self.figurepages) - 1
        self._is_optreturn = isinstance(obj, bt.OptReturn)

        if isinstance(obj, bt.Strategy):
            # only configure plotting for regular backtesting (not for optimization)
            self._configure_plotting(obj)

        if numfigs > 1:
            raise Exception("numfigs must be 1")
        if use is not None:
            raise Exception("Different backends by 'use' not supported")

        self._iplot = iplot and 'ipykernel' in sys.modules

        if isinstance(obj, bt.Strategy):
            self._blueprint_strategy(obj, start, end, tradingdomain, **kwargs)
            if fill_data:
                df: pd.DataFrame = self.build_strategy_data(obj, start, end)

                new_cds = ColumnDataSource.from_df(df)
                append_cds(fp.cds, new_cds)
        elif isinstance(obj, bt.OptReturn):
            # for optresults we only plot analyzers!
            self._cur_figurepage.analyzers += [a for _, a in obj.analyzers.getitems()]
        else:
            raise Exception(f'Unsupported plot source object: {str(type(obj))}')

        return [self._cur_figurepage]

    def show(self):
        """Display a figure (called by backtrader)."""
        # as the plot() function only created the figures and the columndatasources with no data -> now we fill it
        for idx in range(len(self.figurepages)):
            model = self.generate_model(idx)

            if self.p.output_mode in ['show', 'save']:
                if self._iplot:
                    css = self._output_stylesheet()
                    display(HTML(css))
                    show(model)
                else:
                    filename = self._output_plot_file(model, idx, self.p.filename)
                    if self.p.output_mode == 'show':
                        view(filename)
            elif self.p.output_mode == 'memory':
                pass
            else:
                raise RuntimeError(f'Invalid parameter "output_mode" with value: {self.p.output_mode}')

        self._reset()

    def _reset(self):
        self.figurepages = []
        self._is_optreturn = False
    #  endregion
