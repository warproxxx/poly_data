import itertools
from typing import List

from bokeh.models import HoverTool

import backtrader as bt


class HoverContainer(metaclass=bt.MetaParams):
    """Class to store information about hover tooltips. Will be filled while Bokeh glyphs are created. After all figures are complete, hovers will be applied"""

    params = (('hover_tooltip_config', None),
              ('is_multidata', False)
              )

    def __init__(self):
        self._hover_tooltips = []

        self._config = []
        input_config = [] if len(self.p.hover_tooltip_config) == 0 else self.p.hover_tooltip_config.split(',')
        for c in input_config:
            if len(c) != 2:
                raise RuntimeError(f'Invalid hover config entry "{c}"')
            self._config.append((self._get_type(c[0]), self._get_type(c[1])))

    def add_hovertip(self, label: str, tmpl: str, src_obj=None) -> None:
        self._hover_tooltips.append((label, tmpl, src_obj))

    @staticmethod
    def _get_type(t):
        if t == 'd':
            return bt.AbstractDataBase
        elif t == 'i':
            return bt.Indicator
        elif t == 'o':
            return bt.Observer
        else:
            raise RuntimeError(f'Invalid hovertool config type: "{t}')

    def _apply_to_figure(self, fig, hovertool):
        # provide ordering by two groups
        tooltips_top = []
        tooltips_bottom = []
        for label, tmpl, src_obj in self._hover_tooltips:
            apply: bool = src_obj is fig.master  # apply to own
            foreign = False
            if not apply and (isinstance(src_obj, bt.Observer) or isinstance(src_obj, bt.Indicator)) and src_obj.plotinfo.subplot is False:
                # add objects that are on the same figure cause subplot is False (for Indicators and Observers)
                # if plotmaster is set then it will decide where to add, otherwise clock is used
                if src_obj.plotinfo.plotmaster is not None:
                    apply = src_obj.plotinfo.plotmaster is fig.master
                else:
                    apply = src_obj._clock is fig.master
            if not apply:
                for c in self._config:
                    if isinstance(src_obj, c[0]) and isinstance(fig.master, c[1]):
                        apply = True
                        foreign = True
                        break

            if apply:
                prefix = ''
                top = True
                # prefix with data name if we got multiple datas
                if self.p.is_multidata and foreign:
                    if isinstance(src_obj, bt.Indicator):
                        prefix = label_resolver.datatarget2label(src_obj.datas) + " - "
                    elif isinstance(src_obj, bt.AbstractDataBase):
                        prefix = label_resolver.datatarget2label([src_obj]) + " - "
                    top = False

                item = (prefix + label, tmpl)
                if top:
                    tooltips_top.append(item)
                else:
                    tooltips_bottom.append(item)

        # first apply all top hover then all bottoms
        for t in itertools.chain(tooltips_top, tooltips_bottom):
            hovertool.tooltips.append(t)

    def apply_hovertips(self, figures: List['FigureEnvelope']) -> None:
        """Add hovers to to all figures from the figures list"""
        for f in figures:
            for t in f.bfigure.tools:
                if not isinstance(t, HoverTool):
                    continue

                self._apply_to_figure(f, t)
                break

