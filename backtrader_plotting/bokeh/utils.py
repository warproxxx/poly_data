from jinja2 import Environment, PackageLoader

import matplotlib.colors

from backtrader_plotting.utils import nanfilt

from bokeh.models import ColumnDataSource


def convert_color(color):
    """if color is a float value then it is interpreted as a shade of grey and converted to the corresponding html color code"""
    try:
        val = round(float(color) * 255.0)
        hex_string = '#{0:02x}{0:02x}{0:02x}'.format(val)
        return hex_string
    except ValueError:
        return matplotlib.colors.to_hex(color)


def sanitize_source_name(name: str) -> str:
    """removes illegal characters from source name to make it compatible with Bokeh"""
    # Only replace truly problematic characters for Bokeh column names
    # Keep spaces and readable characters for display
    forbidden_chars = '(),.*:/^'
    for fc in forbidden_chars:
        name = name.replace(fc, '_')
    # Replace multiple underscores with single underscore
    while '__' in name:
        name = name.replace('__', '_')
    return name.strip('_')


def get_bar_width() -> float:
    return 0.5


def convert_linestyle(style: str) -> str:
    """Converts a backtrader/matplotlib style string to bokeh style string"""
    style_mpl2bokeh = {
        '-': 'solid',
        '--': 'dashed',
        ':': 'dotted',
        '.-': 'dotdash',
        '-.': 'dashdot',
    }

    return style_mpl2bokeh[style]


def generate_stylesheet(scheme, template="basic.css.j2") -> str:
    env = Environment(loader=PackageLoader('backtrader_plotting.bokeh', 'templates'))
    templ = env.get_template(template)

    css = templ.render(dict(
                             datatable_row_color_even=scheme.table_color_even,
                             datatable_row_color_odd=scheme.table_color_odd,
                             datatable_header_color=scheme.table_header_color,
                             tab_active_background_color=scheme.tab_active_background_color,
                             tab_active_color=scheme.tab_active_color,

                             tooltip_background_color=scheme.tooltip_background_color,
                             tooltip_text_color_label=scheme.tooltip_text_label_color,
                             tooltip_text_color_value=scheme.tooltip_text_value_color,
                             body_background_color=scheme.body_background_color,
                             tag_pre_background_color=scheme.tag_pre_background_color,
                             tag_pre_text_color=scheme.tag_pre_text_color,
                             headline_color=scheme.plot_title_text_color,
                             text_color=scheme.text_color,
                           )
                       )
    return css


def append_cds(base_cds: ColumnDataSource, new_cds: ColumnDataSource):
    updates = []
    for c in new_cds.keys():
        if c not in base_cds.column_names:
            continue
        updates.append((c, new_cds[c]))
    base_cds.data.update(updates)
