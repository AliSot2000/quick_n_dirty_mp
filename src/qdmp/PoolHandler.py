from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError, CancelledError
import datetime
from typing import Callable, Any, List, Tuple, Union
import multiprocessing as mp
from src.qdmp import PoolFuture, PoolFutureException, BaseHandler


class PoolHandler(BaseHandler):
    """
    Base Class for the Reusing Handler. It implements the functionality of enqueueing and tasks for the workers.
    However, the interaction with the workers is not implemented. This is done by the child classes.

    This Class also implements the logging functionality.
    """
    # Interfacing with the child processes
    timeout = 60
    pool: Union[ThreadPoolExecutor, ProcessPoolExecutor, None] = None
    task_list: List[PoolFuture] = None
    __worker_count: int = 0

    def __init__(self, target_fn: Callable, **kwargs):
        super().__init__(target_fn, **kwargs)

    def stop(self, wait: bool = True, cancel_futures: bool = True):
        """
        Stop the Executor pool.

        From the documentation of the Executor
        --------------------------------------

        Signal the executor that it should free any resources that it is using when the currently pending futures are
        done executing. Calls to add_tasks made after stop will raise RuntimeError.

        If wait is True then this method will not return until all the pending futures are done executing and the
        resources associated with the executor have been freed. If wait is False then this method will return
        immediately and the resources associated with the executor will be freed when all pending futures are done
        executing. Regardless of the value of wait, the entire Python program will not exit until all pending futures
        are done executing.

        If cancel_futures is True, this method will cancel all pending futures that the executor has not started
        running. Any futures that are completed or running won’t be cancelled, regardless of the value of
        cancel_futures.

        If both cancel_futures and wait are True, all futures that the executor has started running will be completed
        prior to this method returning. The remaining futures are cancelled.

        You can avoid having to call this method explicitly if you use the with statement, which will shutdown the
        Executor (waiting as if Executor.shutdown() were called with wait set to True):

        :param wait: wait for the tasks to finish before exiting
        :param cancel_futures: cancel the pending futures.

        :return:
        """
        if cancel_futures is True:
            for task in self.task_list:
                task.restart = True
        self.pool.shutdown(wait=wait, cancel_futures=cancel_futures)

    def add_tasks(self, tasks: List[Any]):
        """
        Adds tasks to the queue, the tasks need to be picklable.

        If a validate_args function is provided, the arguments will be validated before being added to the queue.
        Only if the validate_args function returns True, the task is added to the queue. Otherwise it can be added
        regardless.

        :param tasks: List of argument objects to be sent to the worker function
        :return:
        """
        if self.validate_args is not None:
            for t in tasks:
                if self.validate_args(t):
                    self.submit_task(t)
                else:
                    self.logger.debug(f"Task {t} was not added to the queue.")
            return

        for t in tasks:
            self.submit_task(t)

    def process_results(self) -> Tuple[List[Any], List[PoolFutureException]]:
        """
        Traverse the list of currently scheduled tasks. By default, only (2 * number of workers) tasks are checked if
        they are done.

        :return: Tuple of two lists. First list: Results, Second List: Exceptions
        """
        # TODO implement version with limited traversal. For safety, the entire is traversed.

        self.__results = []
        self.__exceptions = []
        remaining_tasks = []

        # traverse the list of tasks and check if they are done.
        for i in range(len(self.task_list)):
            task = self.task_list[i]
            if task.future.done():
                self.__handle_result(task)
            else:
                remaining_tasks.append(task)

                if task.future.running() and task.first_running_time is None:
                    task.first_running_time = datetime.datetime.now()

        return self.__results, self.__exceptions

    def __handle_result(self, task: PoolFuture):
        """
        Function to handle the results of the worker functions. It is called for each future that is done in the
        task_list.

        :param task: PoolFuture object that is done.

        :return:
        """
        # Try to get an exception from the terminated future.
        exception = None
        try:
            exception = task.future.exception(timeout=0.1)

        except TimeoutError as t:
            self.logger.exception(
                f"Timeout error occurred while trying to retrieve exception of task", task.arguments)
            self.__exceptions.append(PoolFutureException(task.arguments, t))

        except CancelledError as t:

            # Task is not being restarted; unexpected cancellation.
            if task.restart is False:
                self.logger.exception(
                    f"Task was cancelled; trying to retrieve exception of task", task.arguments)
                self.__exceptions.append(PoolFutureException(task.arguments, t))

        except Exception as e:
            self.logger.exception(
                f"Unknown exception occurred while trying to retrieve exception of task", task.arguments)
            self.__exceptions.append(PoolFutureException(task.arguments, e))

        # exception occurred.
        if exception is not None:
            # we have an exception handler function. We will call it and check if the exception is fixable.
            if self.handle_error is not None:
                enqueue, new_args, ex_obj = self.handle_error(exception, task.arguments)

                # Exception was determined to be fixable by the handle_error function.
                # A retry will be attempted with potentially modified arguments.
                if enqueue:
                    self.add_tasks([new_args])

                # We have an exception object to be added to the list of exceptions. Check if the returned object is an
                # ProcessingException. If not, we will substitute it with a default ProcessingException.
                else:
                    if not isinstance(ex_obj, PoolFutureException):
                        self.logger.warning(
                            "handle_error function needs to return a ProcessingException object as its "
                            "third return value. Substituting with default")
                        ex_obj = PoolFutureException(task.arguments, exception)

                    self.__exceptions.append(ex_obj)
            else:
                self.__exceptions.append(PoolFutureException(task.arguments, exception))

        try:
            result = task.future.result(timeout=0.1)

            # no exception occurred
            if self.validate_result is not None:
                if self.validate_result(result):
                    self.__results.append(result)
                else:
                    self.logger.debug(f"Result {result} was not added to the results list.")

            # no validation function provided, just add result to the list.
            else:
                self.__results.append(result)

        except TimeoutError as t:
            self.logger.exception(
                f"Timeout error occurred while trying to retrieve result of task", task.arguments)
            self.__exceptions.append(PoolFutureException(task.arguments, t))
        except CancelledError as t:

            # Task is not being restarted; unexpected cancellation.
            if task.restart is False:
                self.logger.exception(
                    f"Task was cancelled; trying to retrieve result of task", task.arguments)
                self.__exceptions.append(PoolFutureException(task.arguments, t))

        except Exception as e:
            self.logger.exception(
                f"Unknown exception occurred while trying to retrieve result of task", task.arguments)
            self.__exceptions.append(PoolFutureException(task.arguments, e))

    def check_process_timeout(self, timeout: int) -> List[PoolFuture]:
        """
        Go through all running tasks and check if they have been running for longer than timeout seconds.

        Compared to the ProcessHandler, the PoolHandler doesn't facilitate the automatic restarting of a given task,
        since a process is taking care of multiple tasks whereas a process in the pool only takes care of a single task
        and therefore the chances are higher that it will fail again. It is left to the programmer to make sure the
        tasks he's cancelling is the right tasks and so is resubmitting the correct arguments.

        :param timeout: number of seconds a task may run max for.
        :return:
        """
        timed_out_futures = []
        for task in self.task_list:
            # Time set and the time is longer than the timeout.
            if task.first_running_time is not None:
                if (datetime.datetime.now() - task.first_running_time).total_seconds() > timeout:
                    timed_out_futures.append(task)

            # Task is running and time is not set. Set the time.
            else:
                if task.future.running():
                    task.first_running_time = datetime.datetime.now()
        return timed_out_futures

    @staticmethod
    def cancel_processes(tasks: List[PoolFuture]):
        """
        Given a list of PoolFutures (subset of the one generated by check_process_timeout), cancel the processes.

        :param tasks: list of futures to cancel.
        :return:
        """
        for task in tasks:
            task.future.cancel()
            task.restart = True

    def submit_task(self, task: Any):
        """
        Submit a task to the pool. The task will be added to the argument queue and will be processed by a child process.
        Task is the arguments for the child process.

        :param task: argument to be passed to the worker function.
        :return:
        """
        ft = self.pool.submit(self.target_fn, task)
        pf = PoolFuture(
            future=ft,
            arguments=task,
            submit_time=datetime.datetime.now(),
        )
        self.task_list.append(pf)

    @property
    def worker_count(self):
        return self.__worker_count


class ThreadPoolHandler(PoolHandler):
    def __init__(self, *args, **kwargs):
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
        super().__init__(*args, **kwargs)

    def start(self, workers: int = None):
        """
        Start the pool of workers. If workers is None, the number of workers will be set to the number of CPU cores.
        :param workers: Number of workers to start. If None, the number of workers will be set to the number of CPU cores.
        :return:
        """
        if workers is None:
            workers = mp.cpu_count()

        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.task_list = []
        self.__worker_count = workers


class ProcessPoolHandler(PoolHandler):
    def __init__(self, *args, **kwargs):
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
        super().__init__(*args, **kwargs)

    def start(self, workers: int = None):
        """
        Start the pool of workers. If workers is None, the number of workers will be set to the number of CPU cores.
        :param workers: Number of workers to start. If None, the number of workers will be set to the number of CPU cores.
        :return:
        """
        if workers is None:
            workers = mp.cpu_count()

        self.pool = ProcessPoolExecutor(max_workers=workers)
        self.task_list = []
        self.__worker_count = workers

