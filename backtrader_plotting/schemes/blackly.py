from .scheme import Scheme


# Custom color palette for cleaner look - buy signals green, sell signals red
custom_colors = [
    '#00ff88',  # 0 - Buy signals (bright green)
    '#ff3366',  # 1 - Sell signals (bright red)
    '#00b8ff',  # 2 - Additional indicators (cyan)
    '#ffaa00',  # 3 - Additional indicators (orange)
    '#bb00ff',  # 4 - Additional indicators (purple)
    '#00ffcc',  # 5 - Additional indicators (teal)
    '#ff6b9d',  # 6 - Additional indicators (pink)
    '#808080',  # 7 - Gray
    '#ffcc00',  # 8 - Yellow
    '#00d4ff',  # 9 - Light blue
]


class Blackly(Scheme):
    def _set_params(self):
        super()._set_params()

        # Modern dark theme colors
        self.crosshair_line_color = '#555555'

        # Improved legend styling - clean and minimal
        self.legend_background_color = '#0d0d0d'
        self.legend_text_color = '#e0e0e0'
        self.legend_location = 'top_left'
        self.legend_orientation = 'vertical'

        # Deep black background with subtle contrast
        self.background_fill = '#0a0a0a'
        self.body_background_color = "#0d0d0d"
        self.border_fill = "#1a1a1a"
        self.legend_click = "hide"

        # Refined axis and grid colors
        self.axis_line_color = '#2a2a2a'
        self.tick_line_color = '#2a2a2a'
        self.grid_line_color = '#1a1a1a'
        self.axis_text_color = '#808080'
        self.plot_title_text_color = '#ffffff'
        self.axis_label_text_color = '#808080'

        # Vibrant bar colors for better contrast
        self.barup = '#00ff88'
        self.bardown = '#ff3366'
        self.barup_wick = '#00cc66'
        self.bardown_wick = '#cc0033'
        self.barup_outline = '#00ff88'
        self.bardown_outline = '#ff3366'

        # Subtle volume colors with better alpha
        self.volup = '#00ff8818'
        self.voldown = '#ff336618'

        # Disable volume plotting completely
        self.volume = False
        self.voloverlay = False

        # Tab styling
        self.tab_active_background_color = '#1a1a1a'
        self.tab_active_color = '#ffffff'

        # Table styling
        self.table_color_even = "#1a1a1a"
        self.table_color_odd = "#0d0d0d"
        self.table_header_color = "#2a2a2a"

        # Modern tooltip
        self.tooltip_background_color = '#1a1a1a'
        self.tooltip_text_label_color = "#00ff88"
        self.tooltip_text_value_color = "#ffffff"

        self.tag_pre_background_color = '#0a0a0a'
        self.tag_pre_text_color = '#e0e0e0'

        # Better aspect ratio for wider charts
        self.plotaspectratio = 2.5

        # Improved Y-axis padding
        self.y_range_padding = 0.1

        # Modern hover tooltip
        self.hovertool_timeformat = '%Y-%m-%d %H:%M'

        # Cleaner number format
        self.number_format = '0,0.0000'

        # Line color for line charts
        self.loc = '#00b8ff'

        # Use custom color palette
        self.lcolors = custom_colors

        # Adjust chart proportions - less space for minor charts (observers)
        self.rowsmajor = 8  # More space for main data chart
        self.rowsminor = 1  # Less space for observers (buy/sell markers)

        # Keep legend visible for Buy/Sell indicators
        self.legend_visible = True
