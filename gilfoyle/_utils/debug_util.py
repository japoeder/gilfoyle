"""
Debugging utilities for Gilfoyle.
"""

import debugpy

debugpy.listen(5678)
debugpy.wait_for_client()


def bp():
    """
    Breakpoint function for Gilfoyle.
    """
    return debugpy.breakpoint()
