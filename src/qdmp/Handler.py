import datetime
from dataclasses import dataclass
from concurrent.futures import Future
from typing import Callable, Any, List
import logging
import os
import sys


@dataclass
class ProcessingException:
    """
    Returned object in case the ProcessHandler runs into an exceptions.

    - Exception - is the exception object generated in the Process
    - Traceback - contains a string of the stack trace of the exception for better understanding of the cause of teh
      exception.
    - Arguments - that were supplied to the process that caused the error.
    """
    exception: Exception
    traceback: str
    arguments: Any


@dataclass
class PoolFuture:
    """
    Wrapper around the normal Future object produced by the concurrent.futures.

    - future - original future
    - arguments - that are being processed by the given future
    - submit_time - time the future was submitted
    - first_running_time - time the future was seen for the first time in the running state instead of the pending.
    - restart - this future is marked for restart. the result handler should discard the result and ignore the fact that
      it is canceled.
    """
    future: Future
    arguments: Any
    submit_time: datetime.datetime
    first_running_time: datetime.datetime = None
    restart: bool = False


@dataclass
class PoolFutureException:
    """
    Returned object in case the PoolHandler runs into an exception.

    Since we don't have a general try, except block, we cannot generate a traceback (or at least I haven't figured out
    how to do so yet).

    So we have a smaller version of the same Exception:
    arguments - the arguments that caused the exception
    exception - the exception raised on the call.
    """
    arguments: Any
    exception: BaseException = None


class BaseHandler:
    # Logging and debugging functionality
    debug: bool = False

    logger: logging.Logger = None
    stream_handler: logging.StreamHandler = None
    file_handler: logging.FileHandler = None
    debug_logger: logging.FileHandler = None

    # funcs
    target_fn: Callable = None
    validate_args: Callable = None
    validate_result: Callable = None
    handle_error: Callable = None

    # for progress the files.
    __results: list = None
    __exceptions: list = None

    def __init__(self, target_fn: Callable, **kwargs):
        """
        Initializes the Reusing Handler.

        optional arguments:
        - debug: If debugging information should be displayed; bool
        - console_level: Level of the console logger; logging.LEVEL
        - logging_name: Name of the logger; str
        - validate_args_fn: Function to validate the arguments; Callable
        - validate_result_fn: Function to validate the results; Callable
        - handle_error_fn: Function to handle errors; Callable

        :param target_fn: function to be executed independently and in parallel.
        :param kwargs:
        """
        self.target_fn = target_fn

        logging_name = f"{self.__class__.__name__}"
        if "logging_name" in kwargs:
            assert type(kwargs["logging_name"]) is str, "logging_name must be a string"
            logging_name = kwargs["logging_name"]

        if "debug" in kwargs:
            assert type(kwargs["debug"]) is bool, "debug must be a boolean"
            self.debug = kwargs["debug"]

        if "validate_args_fn" in kwargs:
            assert callable(kwargs["validate_args_fn"]), "validate_args_fn must be callable"
            self.validate_args = kwargs["validate_args_fn"]

        if "validate_result_fn" in kwargs:
            assert callable(kwargs["validate_result_fn"]), "validate_result_fn must be callable"
            self.validate_result = kwargs["validate_result_fn"]

        if "handle_error_fn" in kwargs:
            assert callable(kwargs["handle_error_fn"]), "handle_error_fn must be callable"
            self.handle_error = kwargs["handle_error_fn"]

        if "console_level" in kwargs:
            assert type(kwargs["console_level"]) is int, "console_level must be an integer"
            self.prepare_logging(kwargs["console_level"], self.debug, logging_name)
        else:
            self.prepare_logging(debug=self.debug, name=logging_name)

    def prepare_logging(self, name: str, console_level: int = logging.WARNING, debug: bool = False):
        """
        Set's up logging for the class.

        :param name: name of the logger
        :param console_level: log level of the console. use logging.LEVEL for this.
        :param debug: store the console log also inside a separate file.
        :return:
        """
        self.logger = logging.getLogger(name)

        # get location for the logs
        fp = os.path.abspath(os.path.dirname(__file__))

        # create two File handlers one for logging directly to file one for logging to Console
        self.stream_handler = logging.StreamHandler(sys.stdout)
        self.file_handler = logging.FileHandler(os.path.join(fp, "execution.log"))

        # create Formatter t o format the logging messages in the console and in the file
        console_formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # add the formatters to the respective Handlers
        self.stream_handler.setFormatter(console_formatter)
        self.file_handler.setFormatter(file_formatter)

        # Set the logging level to the Handlers
        self.stream_handler.setLevel(console_level)
        self.file_handler.setLevel(logging.WARNING)

        # Add the logging Handlers to the logger instance
        self.logger.addHandler(self.stream_handler)
        self.logger.addHandler(self.file_handler)

        # In case a Debug log is desired create another Handler, add the file formatter and add the Handler to the
        # logger
        if debug:
            self.add_debug_logger(file_formatter, fp)

        # We do not want to pollute the information of the above loggers, so we don't propagate
        # We want our logger to emmit every message to all handlers, so we set it to DEBUG.
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def add_debug_logger(self, file_formatter: logging.Formatter, out_dir: str = None):
        """
        Function gets called if prepare_logging has debug set. It can be called alternatively later on during code
        execution if one wants to get a preciser information about what is going on.

        There can only be one debug logger at a time. If this function is called twice, the old logger will be removed
        and the new one will be added. THIS MAY OVERWRITE THE DEBUG FILE IF THE __out_dir__ IS NOT SET!!!

        :param file_formatter: the formatter with which to create the logs.
        :param out_dir: Directory where the logs should be saved
        :return:
        """

        if out_dir is None:
            out_dir = os.path.abspath(os.path.dirname(__file__))

        if self.debug_logger is None:
            self.debug_logger = logging.FileHandler(os.path.join(out_dir, "debug_execution.log"))
            self.debug_logger.setFormatter(file_formatter)
            self.debug_logger.setLevel(logging.DEBUG)
            self.logger.addHandler(self.debug_logger)

        # remove eventually preexisting logger
        else:
            self.logger.removeHandler(self.debug_logger)
            self.add_debug_logger(file_formatter, out_dir)

    def start(self, workers: int = None):
        """
        Placeholder to be implemented by the child classes.
        :param workers: number of workers to spawn
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")

    def stop(self):
        """
        Placeholder to be implemented by the child classes.
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")

    def add_tasks(self, task: List[Any]):
        """
        Placeholder to be implemented by the child classes.
        :param task: List of arguments to be sent to the child processes.
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")
