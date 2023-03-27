import math
from typing import Any, Tuple, Union
from qdmp.Handler import ProcessingException, PoolFutureException


# TODO instead of storage globals?
def sample_target_fn(setup: bool, storage: Any, worker_id: int, debug: bool, arguments: Any) -> (Any, Any):
    """
    Sample target function to be executed by the worker processes. Take this as inspiration.

    :param setup: If this is the first call to the function. If True, the storage should be initialized.
    :param storage: Object passed from the handler to store any information from previous calls of the function.
    :param worker_id: Worker id, used for debugging.
    :param debug: Flag indicating if debugging information should be displayed.
    :param arguments: Arguments to be processed.
    :return: Tuple of storage and result. STORAGE FIRST, RESULT SECOND.
    """

    # Perform setup
    if setup:

        # Initialize storage with 0 just as an example.
        storage = 0
        # Example usage of the debug and worker_id variables
        if debug:
            print(f"{worker_id:02}: Initializing storage")

        return storage, None  # storage is an int, result is None

    # Perform work, return storage and result
    # Use the storage as a method to reuse computation performed in previous calls of the function in THIS WORKER.
    if debug:
        storage += arguments

        print(f"{worker_id:02}: Performing work: {storage}")

    return storage, None  # storage is an int, result is None

def sample_argument_validation_function(arguments: Any) -> bool:
    """
    Sample arguments validation function to be executed by the handler. Take this as inspiration.

    :param arguments: Arguments to be validated.
    :return: True if the arguments are valid, False otherwise.
    """

    # Assume the arguments are integers and an integer is only valid, if it is a multiple of 13
    # Of course you can have much more elaborate functions here.
    # Say you want to check if a file path exists, or if a file is a valid image, etc.
    return arguments % 13 == 0

def sample_results_validation_function(results: Any) -> bool:
    """
    Sample results validation function to be executed by the handler. Take this as inspiration.

    :param results: Results to be validated.
    :return: True if the results are valid, False otherwise.
    """

    # Assume the results are integers and an integer is only valid, if it is a square number
    # Of course you can have much more elaborate functions here.
    # You could check if a file now exists, or an output is produced by a function, etc.
    return int(math.sqrt(results)) - math.sqrt(results) == 0

def sample_error_handling_function_process(exception: Exception, arguments: Any, traceback: str = None) \
        -> Tuple[bool, Any, Union[ProcessingException, None]]:
    """
    Sample error handling function to be executed by the handler. Take this as inspiration.

    :param exception: Exception that was raised by the target function.
    :param arguments: Arguments that were associated with the target function and the exception.
    :param traceback: Stacktrace of the function call.

    - enqueue: If True, the arguments will be enqueued again. If False, the arguments will be discarded.
    - new_arguments: New arguments to be enqueued. Enqueueing depends on enqueue.
    - processing_exception: ProcessingException to be piped out. Only added to the exception list if enqueue is False.

    If enqueue is True, new_arguments will be validated by the validate_arguments function again.

    :return: enqueued, new_arguments, processing_exception.
    """
    # Assume we had some error and the File was created twice. We cannot fix that, so we return an Exception.
    if isinstance(exception, FileExistsError):
        print(f"File already exists: {arguments}")
        print(f"Traceback: {traceback}")

        # We return False to indicate that the error was not recoverable and we don't want to add new arguments.
        # We don't have any new arguments to return, so we return None.
        # We return the ProcessingException to be piped out as the result.
        return False, None, ProcessingException(exception, traceback, arguments)

    elif isinstance(exception, ValueError):
        print(f"ValueError: {arguments}")

        # We return True to indicate that the error was recoverable and we want to add new arguments.
        # We return the new arguments to be added to the queue.
        # We return the ProcessingException to be piped out as the result.
        return True, arguments + 1, None

    else:
        # We return False to indicate that the error was not recoverable and we don't want to add new arguments.
        # We don't have any new arguments to return, so we return None.
        # We return the ProcessingException to be piped out as the result.
        return False, None, ProcessingException(exception, traceback, arguments)


def sample_error_handling_function_pool(exception: Exception, arguments: Any) \
        -> Tuple[bool, Any, Union[PoolFutureException, None]]:
    """
    Sample error handling function to be executed by the handler. Take this as inspiration.

    :param exception: Exception that was raised by the target function.
    :param arguments: Arguments that were associated with the target function and the exception.

    - enqueue: If True, the arguments will be enqueued again. If False, the arguments will be discarded.
    - new_arguments: New arguments to be enqueued. Enqueueing depends on enqueue.
    - processing_exception: ProcessingException to be piped out. Only added to the exception list if enqueue is False.

    If enqueue is True, new_arguments will be validated by the validate_arguments function again.

    :return: enqueued, new_arguments, processing_exception.
    """
    # Assume we had some error and the File was created twice. We cannot fix that, so we return an Exception.
    if isinstance(exception, FileExistsError):
        print(f"File already exists: {arguments}")

        # We return False to indicate that the error was not recoverable and we don't want to add new arguments.
        # We don't have any new arguments to return, so we return None.
        # We return the ProcessingException to be piped out as the result.
        return False, None, PoolFutureException(arguments, exception)

    elif isinstance(exception, ValueError):
        print(f"ValueError: {arguments}")

        # We return True to indicate that the error was recoverable and we want to add new arguments.
        # We return the new arguments to be added to the queue.
        # We return the ProcessingException to be piped out as the result.
        return True, arguments + 1, None

    else:
        # We return False to indicate that the error was not recoverable and we don't want to add new arguments.
        # We don't have any new arguments to return, so we return None.
        # We return the ProcessingException to be piped out as the result.
        return False, None, PoolFutureException(arguments, exception)