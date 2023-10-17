from fileio.utils import logger

# MPU seems to work > 150MB?
_debug_mode: bool = False

def _log(f, *args, **kwargs):
    if _debug_mode:
        logger.info(f, *args, **kwargs)
    else:
        logger.debug(f, *args, **kwargs)

