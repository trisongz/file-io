from fileio.utils import logger

# MPU seems to work > 150MB?
_debug_mode: bool = True

def _log(f, *args, **kwargs):
    if _debug_mode:
        logger.debug(f, *args, **kwargs)
    else:
        logger.info(f, *args, **kwargs)

