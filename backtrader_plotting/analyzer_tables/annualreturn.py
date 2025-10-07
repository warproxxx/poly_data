from backtrader_plotting.bokeh.datatable import ColummDataType


def datatable(self):
    cols1 = [['Year', ColummDataType.STRING], ['Return', ColummDataType.FLOAT]]

    if getattr(self, 'ret', None) is None:
        # analyzer is only filled on 'stop'. So empty in live mode
        cols1[0].append('No data available. Live mode?')
        cols1[1].append('0.0')
    else:
        a = self.get_analysis()

        for k, v in a.items():
            cols1[0].append(k)
            cols1[1].append(v)

    return "Annual Return", [cols1]
