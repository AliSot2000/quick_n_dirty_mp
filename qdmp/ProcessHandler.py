import datetime
from typing import Callable, Any, List, Tuple, Union
import multiprocessing as mp
from multiprocessing.connection import Connection
from enum import Enum
import queue
import traceback
import sys
import logging
import os
import warnings
from qdmp.Handler import ProcessingException, BaseHandler


# TODO TEST!!!
Commands = Enum("Commands", ["RESET_SANITY_TIMEOUT", "TERMINATE"])


def worker(w_id: int, input_queue: mp.Queue, output_queue: mp.Queue, com: Connection, target_fn: Callable,
           sanity_timeout: int = 600, debug: bool = False):
    """
    Worker function instantiated by the Reusing Handler class. It handles the communication with the parent process.

    :param debug: If Debugging information should be displayed.
    :param target_fn: Function to execute. Must return a tuple of (storage, result)
    :param w_id: worker id
    :param input_queue: Queue where the arguments are sent in.
    :param output_queue: Queue where the results are sent out.
    :param com: Communication channel to the parent process.
    :param sanity_timeout: Timeout in seconds after which the worker will terminate if no new arguments are received.
    :return:
    """

    assert sanity_timeout > 0, "Sanity timeout must be greater than 0"

    storage: Any
    storage, _ = target_fn(setup=True, storage=None, worker_id=w_id, debug=debug, arguments=None)

    timeout = 0

    while True:
        # poll the communication channel for a message
        if com.poll():
            command = com.recv()

            if command == Commands.RESET_SANITY_TIMEOUT:
                timeout = 0
                print(f"{w_id:02}: Resetting sanity timeout")
            elif command == Commands.TERMINATE:
                print(f"{w_id:02}: Terminating")
                break

        # poll the input queue for a message
        try:
            args = input_queue.get(timeout=1)
            timeout = 0
        except queue.Empty:
            timeout += 1

            if timeout > sanity_timeout:
                print(f"{w_id:02}: Sanity timeout reached. Terminating")
                break

            continue

        # execute the function with general try catch to be sure the worker doesn't die
        try:
            storage, result = target_fn(setup=False, storage=storage, worker_id=w_id, debug=debug, arguments=args)
            # Reset traceback since it is not needed anymore.
            # Reset the args since they don't need to be transmitted back.
            tb = None
            args = None
            e = None

        # Handle exception.
        except Exception as e:
            print(f"{w_id:02}: Exception occurred: {e}", file=sys.stderr, flush=True)
            tb = traceback.format_exc()
            # Reset the result since it is not needs from the last call.
            result = None

            if debug:
                print(f"{w_id:02}: Traceback: \n{tb}", file=sys.stderr, flush=True)

        # Add the result to the output queue.
        output_queue.put({"result": result, "traceback": tb, "arguments": args, "worker_id": w_id, "exception": e})


class ReusingHandler(BaseHandler):
    """
    Base Class for the Reusing Handler. It implements the functionality of enqueueing and tasks for the workers.
    However, the interaction with the workers is not implemented. This is done by the child classes.

    This Class also implements the logging functionality.
    """
    # Interfacing with the child processes
    children: Union[list, None] = None
    com_pipe: Union[List[Connection], None] = None
    last_interaction: Union[List[datetime.datetime], None] = None
    timeout = 60

    __arg_queue: mp.Queue = None
    __res_queue: mp.Queue = None

    # for logging and warnings
    __returned_task_queue: bool = False

    def __init__(self, target_fn: Callable, **kwargs):
        """
        Initializes the Reusing Handler.

        optional arguments:
        - debug: If debugging information should be displayed; bool
        - console_level: Level of the console logger; logging.LEVEL
        - logging_name: Name of the logger; str

        :param target_fn: function to be executed independently and in parallel.
        :param kwargs:
        """
        super().__init__(target_fn, **kwargs)
        self.__arg_queue = mp.Queue()
        self.__res_queue = mp.Queue()

    def start(self, workers: int = None):
        """
        Stub function to be implemented by the child classes.
        :param workers: number of workers to spawn
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")

    def restart_child(self, w_id: int):
        """
        Stub function to be implemented by the child classes.
        :param w_id: specific child to restart
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")

    def check_processes(self) -> Tuple[bool, bool, bool, bool, List[Union[int, None]]]:
        """
        Stub function to be implemented by the child classes.
        :return:
        """
        raise TypeError("Parent class, not intended to be instantiated")

    def reset_sanity_timer(self):
        """
        Resets the sanity timer of all children.
        :return:
        """
        if self.com_pipe is None:
            self.logger.warning("No children running. Cannot reset sanity timer.")
            return

        for p in self.com_pipe:
            p.send(Commands.RESET_SANITY_TIMEOUT)

    def send_termination_signal(self):
        """
        Sends a termination signal to all children.
        :return:
        """
        if self.com_pipe is None:
            self.logger.warning("No children running. Cannot reset sanity timer.")
            return

        for p in self.com_pipe:
            p.send(Commands.TERMINATE)

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
                    self.__arg_queue.put(t)
                else:
                    self.logger.debug(f"Task {t} was not added to the queue.")
            return

        for t in tasks:
            self.__arg_queue.put(t)

    def process_results(self, count: int = 1000) -> Tuple[List[Any], List[ProcessingException]]:
        """
        Process will attempt to dequeue up to count elements from the results queue. If the queue is empty, the
        dequeue will stop. If count is None, all elements in the queue will be dequeued.

        :param count: Number of elements to dequeue from the results queue. If None, all available elements will be
        dequeued.
        :return: Tuple of two lists. First list: Results, Second List: Exceptions
        """
        if count < 1:
            raise ValueError("Number of Elements to dequeue needs to be greater than one.")

        self.__results = []
        self.__exceptions = []

        # case where we have a while loop to dequeue all elements
        if count is None:
            avail = True

            while avail:
                avail = self.__handle_result()

            return self.__results, self.__exceptions

        # case where we have a for loop to dequeue upto a certain number of elements
        for _ in range(count):
            if not self.__handle_result():
                break

        return self.__results, self.__exceptions

    def __handle_result(self) -> bool:
        """
        Function to handle the results of the worker function. It is called for each result that is dequeued
        from the results queue.

        :return: True if a result was dequeued, False otherwise.
        """
        try:
            row = self.__res_queue.get(block=False)
        except queue.Empty:
            return False

        result = row["result"]
        tb = row["traceback"]
        args = row["arguments"]
        w_id = row["worker_id"]
        e = row["exception"]

        # update information about child process
        self.last_interaction[w_id] = datetime.datetime.now()

        # exception occurred.
        if e is not None:
            if self.handle_error is not None:
                enqueue, new_args, exception = self.handle_error(e, args, tb)

                # Exception was determined to be fixable by the handle_error function.
                # A retry will be attempted with potentially modified arguments.
                if enqueue:
                    self.add_tasks([new_args])

                # We have an exception object to be added to the list of exceptions. Check if the returned object is an
                # ProcessingException. If not, we will substitute it with a default ProcessingException.
                else:
                    if not isinstance(exception, ProcessingException):
                        self.logger.warning("handle_error function needs to return a ProcessingException object as its "
                                        "third return value. Substituting with default")
                        exception = ProcessingException(e, tb, args)

                    self.__exceptions.append(exception)
            else:
                self.__exceptions.append(ProcessingException(e, tb, args))

            return True

        # no exception occurred
        if self.validate_result is not None:
            if self.validate_result(result):
                self.__results.append(result)
            else:
                self.logger.debug(f"Result {result} was not added to the results list.")

        # no validation function provided, just add result to the list.
        else:
            self.__results.append(result)

        return True

    def check_process_timeout(self, timeout: int) -> List[int]:
        """
        Go through all processes and make sure they had a result published into the results queue within the
        seconds specified in timeout.

        :param timeout: number of seconds the last task was processed by a child process.
        :return: List of process ids that have not been active for more than timeout seconds.
        """
        unknown_process_ids = []
        for w_id in range(len(self.last_interaction)):
            last_interaction = self.last_interaction[w_id]
            if (datetime.datetime.now() - last_interaction).total_seconds() > timeout:
                self.logger.warning(f"Worker {w_id} has not been active for more than {timeout} seconds. "
                                    f"Killing process.")
                unknown_process_ids.append(w_id)

        return unknown_process_ids

    def restart_timed_out_children(self, timeout: int):
        """
        Restart all children that have not been active for more than timeout seconds.
        :param timeout: number of seconds the last task was processed by a child process.
        :return:
        """
        unknown_process_ids = self.check_process_timeout(timeout)
        for pid in unknown_process_ids:
            self.restart_child(pid)

    @property
    def argument_queue(self):
        """
        It is a valid possible to get the queue that sends arguments to the child processes.
        This function is used to emit a warning if you also have a validation function for the arguments.
        :return:
        """
        if self.validate_args is not None and self.__returned_task_queue is False:
            warnings.warn("You are using a custom argument validation function. \n"
                          "Arguments added to the queue will not be automatically validated.")

        self.__returned_task_queue = True
        return self.__arg_queue


class ProcessHandler(ReusingHandler):
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
        super().__init__(target_fn, **kwargs)

    def start(self, workers: int = None):
        """
        Start the processes and the communication pipes.
        :param workers: Number of workers to start. Default: number of workers will be set to the number of CPU cores.
        :return:
        """
        if self.com_pipe is not None:
            self.logger.warning("Children already running. Stop the children first.")
            return

        if workers is None:
            workers = mp.cpu_count()

        elif workers < 1:
            raise ValueError("Workers must be at least 1")

        self.children = []
        self.com_pipe = []
        self.last_interaction = []

        for i in range(workers):
            parent, child = mp.Pipe()
            p = mp.Process(target=worker, args=(i, self.__arg_queue, self.__res_queue, child, self.target_fn, self.timeout,
                                                self.debug))
            p.start()
            self.children.append(p)
            self.com_pipe.append(parent)
            self.last_interaction.append(datetime.datetime.now())

    def restart_child(self, w_id: int):
        """
        Restart a child process. This will kill the current process and start a new one.
        :param w_id: id of the child process to be restarted.
        :return:
        """
        if self.com_pipe is None:
            self.logger.warning("Children not running. Cannot restart.")
            return

        if w_id < 0 or w_id >= len(self.children):
            raise ValueError(f"Worker id {w_id} is not valid. Valid ids are 0 to {len(self.children) - 1}")

        self.logger.warning(f"Restarting child {w_id}. You may loose the results of the current task in progress")

        # kill the process
        self.children[w_id].kill()

        # create a new process
        parent, child = mp.Pipe()
        p = mp.Process(target=worker, args=(w_id, self.__arg_queue, self.__res_queue, child, self.target_fn, self.timeout,
                                            self.debug))
        p.start()
        self.children[w_id] = p
        self.com_pipe[w_id] = parent
        self.last_interaction[w_id] = datetime.datetime.now()

    def stop(self, waiting_timeout: int = 1):
        """
        Stop the processes and the communication pipes.
        :param waiting_timeout: number of seconds to wait until the process is killed.
        :return:
        """

        if self.com_pipe is None:
            self.logger.warning("No children running. Nothing to stop.")
            return

        if waiting_timeout < 1:
            raise ValueError("Waiting timeout must be at least 1 second.")

        self.send_termination_signal()

        for i in range(len(self.children)):
            p = self.children[i]
            try:
                self.logger.info(f"Trying to join process {i}. Process Alive State is {p.is_alive()}")
                p.join(waiting_timeout)
                if p.is_alive():
                    self.logger.info(f"Process {i} joined but still alive; killing it.")
                    p.kill()
            except TimeoutError:
                self.logger.warning(f"Process {i} timed out. Alive state: {p.is_alive()}; killing it.")
                p.kill()

        self.children = None
        self.com_pipe = None
        self.last_interaction = None

    def check_processes(self) -> Tuple[bool, bool, bool, bool, List[Union[int, None]]]:
        """
        Checks up on the processes. exited and error, are true if AT LEAST ONE process is exited or experienced an
        exception. all_exited and all_error are true if ALL processes are exited or experienced an exception.
        exit_codes is a list of the exit codes of the processes. If a process is still running, the exit code is None.

        Given the structure of the code, error and all_error shouldn't really be possible,
        but it is added as a precaution.

        :return: error, all_error, exited, all_exited, exit_codes
        """
        exited = False
        all_exited = True
        error = False
        all_error = False
        exit_codes = []

        for p in self.children:
            # if it is running, it has not exited and not errored
            if not p.is_alive():
                e = p.exitcode
                exit_codes.append(e)

                if e is not None:
                    if e != 0:
                        error = True
                    else:
                        all_error = False

                else:
                    self.logger.warning("process is not alive but no exit code available")

            else:
                all_error = False
                all_exited = False

        return error, all_error, exited, all_exited, exit_codes


# TODO implement same with threads.
#   However, threads aren't as versatile since they don't allow killing of running threads etc.
