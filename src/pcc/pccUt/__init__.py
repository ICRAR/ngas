#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: __init__.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/06/2001  Created


"""
The pccUt module contains various utility classes and functions.
"""
import sys, pcc
__all__ = []
__all__.extend(['PccUtDb2KeyMap',
                'PccUtDefs',
                'PccUtGenClassHierach',
                'PccUtList',
                'PccUtString',
                'PccUtTime',
                'PccUtUtils'
                ])

pcc.__all__.extend(__all__)

def public(f):
    """"Use a decorator to avoid retyping function/class names.

    * Based on an idea by Duncan Booth:
    http://groups.google.com/group/comp.lang.python/msg/11cbb03e09611b8a
    * Improved via a suggestion by Dave Angel:
    http://groups.google.com/group/comp.lang.python/msg/3d400fb22d8a42e1
    """
    all = sys.modules[f.__module__].__dict__.setdefault('__all__', [])
    if f.__name__ not in all:  # Prevent duplicates if run from an IDE.
        all.append(f.__name__)
    return f
public(public)  # Emulate decorating ourself


# --- oOo ---
