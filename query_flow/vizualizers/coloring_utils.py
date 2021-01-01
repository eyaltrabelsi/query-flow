import random

from colour import Color
from colour import RGB_TO_COLOR_NAMES


def optimize_color(color, by='luminance', value=0.75):
    """
    >>> optimize_color("red")
    '#ff7f7f'

    >>> optimize_color("green")
    '#7fff7f'

    >>> optimize_color("yellow")
    '#ffff7f'
    """

    c = Color(color)
    if by == 'luminance':
        c.luminance = value
    elif by == 'saturation':
        c.saturation = value
    else:
        raise ValueError(f'Optimization by {value} is not supported')
    return c.hex


def color_range(c, n, by='luminance', min_value=0.5, max_value=0.9):
    """
    >>> list(color_range("red", 4))
    ['#f00', '#f33', '#f66', '#f99']

    >>> list(color_range("red", 3))
    ['#f00', '#f44', '#f88']
    """

    increment = (max_value - min_value) / n
    for i in range(n):
        value = min_value + i * increment
        yield optimize_color(c, by, value)


def sample_colors(n):
    """
    >>> sample_colors(0)
    []
    >>> len(sample_colors(2))
    2
    """
    if n <= 7:
        colors = ['yellow', 'green', 'khaki',
                  'red', 'purple', 'orange', 'silver']
    else:
        colors = [color[0] for color in RGB_TO_COLOR_NAMES.values()]
        random.shuffle(colors)
    return colors[:n]


if __name__ == '__main__':
    import doctest
    doctest.testmod()
