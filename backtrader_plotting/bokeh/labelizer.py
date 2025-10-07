import operator

import backtrader as bt


def _operator2string(op) -> str:
    opmap = {
        operator.sub:'-',
        operator.add: '+',
        operator.mul: '*',
        operator.truediv: '/',
        operator.gt: '>',
        operator.ge: '>=',
        operator.lt: '<',
        operator.le: '<=',
        operator.eq: '=',
        operator.mod: '%',
        operator.ne: '!=',
    }
    return opmap[op] if op in opmap else '?'


def _get_line_alias(line, owner):
    """Get the line alias of the passed line object by searching in the owner lines"""
    for idx, curline in enumerate(owner.lines):
        if curline is not line:
            continue
        return owner._getlinealias(idx)
    raise RuntimeError('Could not find line in owner lines')


def _clean_label(text):
    """Clean up label text by removing underscores and improving readability"""
    # Replace underscores with spaces
    text = text.replace('_', ' ')
    # Remove common technical prefixes/suffixes
    text = text.replace('SimpleMovingAverage', 'MA')
    text = text.replace('ExponentialMovingAverage', 'EMA')
    text = text.replace('MovingAverage', 'MA')
    return text.strip()


def _label_datafeed(data):
    """Convert datas (usually a datafeed but might also be an indicator if one indicator operates on another indicator) to a readable string.
    If a name was provided manually then use that."""

    # try some popular attributes that might carry string representations
    # _name: user assigned value upon instantiation
    # shortname: used by some datafeeds
    # _dataname: underlying bt dataname (should always be available as last resort)
    prim_names = ['_name', 'shortname', '_dataname']
    labels = []
    for n in prim_names:
        val = getattr(data, n, "")
        if val is None:
            continue
        val = str(val)

        if len(val) > 0:
            labels.append(_clean_label(val))
            break
        else:
            raise RuntimeError(f'Unexpected data type: {data.__class__}')

    if len(labels) == 0:
        return "Unidentified"
    return ','.join(labels)


def line2str(line, owner):
    if isinstance(owner, bt.AbstractDataBase):
        # line of a data feed
        return _label_datafeed(owner) + '^' + _get_line_alias(line, owner)  # we basically assume to only hae 1 line here
    elif isinstance(owner, bt.IndicatorBase):
        return label(line)
    elif isinstance(line, bt.LinesOperation):
        return label(line)
    elif isinstance(line, bt.linebuffer._LineDelay):
        return label(line)
    else:
        raise RuntimeError('Unexpected type')


def label(obj, targets=True):
    if isinstance(obj, (bt.IndicatorBase, bt.ObserverBase)):
        primary = _clean_label(obj.plotlabel())
    elif isinstance(obj, bt.LinesOperation):
        op = _operator2string(obj.operation)
        primary = f'LineOp{op}'
    elif isinstance(obj, bt.linebuffer._LineDelay):
        primary = _label_datafeed(obj.a._owner) + '^' + _get_line_alias(obj.a, obj.a._owner) + f'({obj.ago})'
    elif isinstance(obj, bt.LineSeriesStub):
        primary = line2str(obj.lines[0], obj._owner)
    elif isinstance(obj, bt.AbstractDataBase):
        primary = _label_datafeed(obj)
    elif isinstance(obj, bt.LineBuffer):
        if isinstance(obj._owner, bt.AbstractDataBase):
            prefix = _label_datafeed(obj._owner)
        elif isinstance(obj._owner, bt.MultiCoupler):
            prefix = label(obj._owner.data)
        else:
            prefix = _clean_label(obj._owner.plotlabel())
        primary = prefix + '^' + _get_line_alias(obj, obj._owner)
    elif isinstance(obj, bt.MultiCoupler):
        primary = f'Coupler'
    elif isinstance(obj, (int, float)):  # scalar
        return str(obj)
    else:
        raise RuntimeError(f'Unsupported type: {obj.__class__.__name__}')

    # targets
    target_datas = []
    if targets:
        if isinstance(obj, (bt.Indicator, bt.Observer, bt.MultiCoupler)):
            target_datas = obj.datas
        elif isinstance(obj, bt.LinesOperation):
            target_datas = [obj.a, obj.b]
        elif isinstance(obj._owner, bt.Indicator):
            target_datas = obj._owner.datas

    targets = []
    for d in target_datas:
        targets.append(label(d))

    total = primary
    if len(targets) > 0:
        total += ' @ (' + ', '.join(targets) + ')'

    return total
