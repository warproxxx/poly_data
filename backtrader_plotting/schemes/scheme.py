from backtrader_plotting.schemes.btscheme import PlotScheme as _BtPlotScheme


class Scheme(_BtPlotScheme):
    def __init__(self, **kwargs):
        super().__init__()
        self._set_params()
        self._set_args(**kwargs)

    def _set_params(self):
        """
        hover tooltips of datas will contain all other datas and all indicators/observers
        if set to False then tooltips of datas will only contain the current data and indicators/observers related to that data
        """
        self.tabs = 'single'
        self.show_headline = True
        self.hover_tooltip_config = ''

        self.barup_wick = self.barup
        self.bardown_wick = self.bardown

        self.barup_outline = self.barup
        self.bardown_outline = self.bardown

        self.crosshair_line_color = '#999999'

        self.legend_background_color = '#3C3F41'
        self.legend_text_color = 'lightgrey'
        self.legend_location = 'top_left'
        self.legend_orientation = 'horizontal'

        self.loc = 'lightgray'
        self.background_fill = '#222222'
        self.body_background_color = "white"
        self.border_fill = "#3C3F41"
        self.legend_click = "hide"  # or "mute"
        self.axis_line_color = 'darkgrey'
        self.tick_line_color = self.axis_line_color
        self.grid_line_color = '#444444'
        self.axis_text_color = 'lightgrey'
        self.plot_title_text_color = 'darkgrey'
        self.axis_label_text_color = 'darkgrey'
        self.tag_pre_background_color = 'lightgrey'
        self.tag_pre_text_color = 'black'

        self.xaxis_pos = "all"  # "all" or "bottom"

        self.table_color_even = '#404040'
        self.table_color_odd = '#333333'
        self.table_header_color = '#7a7a7a'

        self.analyzer_tab_num_cols = 2
        self.plotaspectratio = 3.0
        self.plot_sizing_mode = "scale_width"

        self.toolbar_location = "right"

        self.tooltip_background_color = '#4C4F51'
        self.tooltip_text_label_color = '#848EFF'
        self.tooltip_text_value_color = '#aaaaaa'

        self.tab_active_background_color = '#333333'
        self.tab_active_color = '#4C4F51'

        self.text_color = 'lightgrey'

        # https://docs.bokeh.org/en/latest/docs/reference/models/formatters.html#bokeh.models.formatters.DatetimeTickFormatter
        self.hovertool_timeformat = '%F %R'

        self.number_format = '0,0.000'
        self.number_format_volume = '0.00 a'

        # https://docs.bokeh.org/en/latest/docs/reference/models/formatters.html
        self.axis_tickformat_days = "%d %b %R"
        self.axis_tickformat_hourmin = "%H:%M:%S"
        self.axis_tickformat_hours = "%d %b %R"
        self.axis_tickformat_minsec = "%H:%M:%S"
        self.axis_tickformat_minutes = "%H:%M"
        self.axis_tickformat_months = "%d/%m/%y"
        self.axis_tickformat_seconds = "%H:%M:%S"
        self.axis_tickformat_years = "%Y %b"

        self.y_range_padding = 0.5  # used to add padding on the y-axis for all data except volume

    def _set_args(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise Exception(f'Invalid scheme parameter "{k}')
            setattr(self, k, v)
