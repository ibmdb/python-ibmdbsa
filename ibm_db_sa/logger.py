import logging as ibmdbsa_logging
import functools
import inspect
logger = ibmdbsa_logging.getLogger("ibm_db_sa")
logger.setLevel(ibmdbsa_logging.DEBUG)
logger.propagate = False  # prevent propagation to root logger

def configure_ibmdbsa_logging(target=False):
   """
   Configure ibm_db_sa logging.
   target = True      -> console logging
   target = "file"    -> file logging (overwrite file)
   target = False     -> disable logging
   """
   # Prevent reconfiguration if already configured with same target
   current_target = getattr(logger, "_ibmdbsa_target", None)
   if current_target == target:
       return
   # Remove existing handlers
   for handler in list(logger.handlers):
       logger.removeHandler(handler)
       try:
           handler.close()
       except Exception:
           pass
   if not target:
       logger.disabled = True
       logger._ibmdbsa_target = target
       return
   # Console logging
   if target is True:
       handler = ibmdbsa_logging.StreamHandler()
   # File logging
   elif isinstance(target, str):
       handler = ibmdbsa_logging.FileHandler(target, mode="w")
   else:
       logger.disabled = True
       logger._ibmdbsa_target = target
       return
   formatter = ibmdbsa_logging.Formatter(
       "%(asctime)s - [ibm_db_sa] - %(levelname)s - %(message)s",
       "%Y-%m-%d %H:%M:%S"
   )
   handler.setFormatter(formatter)
   logger.addHandler(handler)
   logger.disabled = False
   logger._ibmdbsa_target = target
   logger.debug(f"IBM_DB_SA logging initialized -> {target}")

def init_ibmdbsa_logging(url):
   """
   Extract 'ibmdbsa_log' from SQLAlchemy URL query parameters,
   configure logging if present, and remove the parameter
   so it is not passed to the DBAPI layer.
   """
   ibmdbsa_log_value = None
   try:
       query_keys = list(url.query.keys()) if url.query else []
   except Exception:
       query_keys = []
   for qk in query_keys:
       if qk.lower() == "ibmdbsa_log":
           raw_val = url.query[qk]
           if isinstance(raw_val, str):
               val = raw_val.lower()
               if val in ("true", "1", "yes", "y"):
                   ibmdbsa_log_value = True
               elif val in ("false", "0", "no", "n", ""):
                   ibmdbsa_log_value = False
               else:
                   ibmdbsa_log_value = raw_val
           else:
               ibmdbsa_log_value = raw_val
           # remove parameter so DBAPI never receives it
           url = url.difference_update_query([qk])
           break
   if ibmdbsa_log_value is not None:
       configure_ibmdbsa_logging(ibmdbsa_log_value)
       logger.debug(
           f"ibm_db_sa logging enabled via URL parameter -> {ibmdbsa_log_value}"
       )
   return url, ibmdbsa_log_value

def log_entry_exit(func):
   """Logs entry, exit, execution time, and exceptions."""
   import time
   @functools.wraps(func)
   async def async_wrapper(*args, **kwargs):
       start = time.time()
       try:
           logger.info(f"Entry: {func.__name__}")
           result = await func(*args, **kwargs)
           duration = round((time.time() - start) * 1000, 2)
           logger.info(f"Exit: {func.__name__} (took {duration} ms)")
           return result
       except Exception as e:
           logger.exception(f"Exception in {func.__name__}: {e}")
           raise
   @functools.wraps(func)
   def sync_wrapper(*args, **kwargs):
       start = time.time()
       try:
           logger.info(f"Entry: {func.__name__}")
           result = func(*args, **kwargs)
           duration = round((time.time() - start) * 1000, 2)
           logger.info(f"Exit: {func.__name__} (took {duration} ms)")
           return result
       except Exception as e:
           logger.exception(f"Exception in {func.__name__}: {e}")
           raise
   return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper

def _format_args(args, kwargs):
   parts = []
   if args:
       parts.append(", ".join(map(str, args)))
   if kwargs:
       parts.append(", ".join(f"{k}={v}" for k, v in kwargs.items()))
   return ", ".join(parts)

__all__ = [
   "logger",
   "configure_ibmdbsa_logging",
   "init_ibmdbsa_logging",
   "log_entry_exit"
]