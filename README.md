# Quick and Dirty Multiprocessing

### Motivation
This package is a collection of Handlers for multiprocessing in a Producer-Consumer situation. It also assumes the 
preparation of the arguments as well as the validating of the arguments is _significantly faster_ than the actual
computation. 

Additionally, all Handlers intended to allow a stream like processing of tasks. You submit the arguments and then 
retrieve the results. Even though modern systems have a large amount of RAM, for excessively large amounts of tasks with
a very small amount of RAM it might not be possible to store all the arguments in memory which wouldn't allow the usage 
of the map function (ML Applications for example). 

Furthermore, the ability to submit and retrieve results in a stream like fashion allows for better integration in 
asynchronous workloads where a multiple child nodes receive tasks from a master at random intervals.

The package is designed around the `concurrent.futures` Executor but also has its own implementation of a Pool.
The Pool can be of use if you have a part of the computation that can be reused as the `Processes` live on whereas the 
`Executor` kills the `Processes` afterwards. Though it should be noted that using this pool will only result in marginal 
improvements because the pool has no information about the relation of the precomputed data. To achieve even better 
performance it is advisable to implement a custom `Executor` or `Pool` that can take full advantage of the precomputed 
data.

### Basic usage

```python
from qdmp import ProcessPoolHandler
from typing import Any


def target_function(arguments: Any) -> Any:
  """
  Sample target function to be executed by the handler. Take this as inspiration.

  :param arguments: Arguments to be processed.
  :return: Results of the computation.
  """
  return arguments ** 2


# Create a handler with 4 processes
handler = ProcessPoolHandler(target_function)
handler.start(4)
handler.add_tasks([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
results, errors = handler.process_results()
handler.stop()
```

## Available Handlers:
- `ProcessPoolHandler` A Wrapper around the `ProcessPoolExecutor` from the `concurrent.futures` package.
- `ThreadPoolHandler` A Wrapper around the `ThreadPoolExecutor` from the `concurrent.futures` package.
- `PoolHandler` A custom implementation of a Pool. This is a bit slower than the `ProcessPoolHandler` but allows for 
  precomputing data that can be reused in the computation. 

### Additional Features
The package also allows for validation of the arguments and the results as well as user definable error handling.

##### Validation
Provide a function that accepts the arguments you pass into the Handler and returns a boolean. 
The boolean determines if the arguments are passed on to be processed or are discarded. 
(True = Schedule, False = Discard)

```python
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
```

##### Result Validation
Provide a function that accepts the result of the computation and returns a boolean.
The boolean determines if the result is passed on to be returned or is discarded.
(True = Return, False = Discard)

```python
import math

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
```

##### Error Handling
Provide a function that accepts the arguments and the error that was raised during the computation. 
The function then should determine if the encountered error is recoverable or not. 
Depending on the decision, the function must pass on new arguments (which can be the old ones or modified ones) or an Exception 
object to be returned.

```python
from typing import Tuple, Union, Any
from src.qdmp import ProcessingException, PoolFutureException


# Error handlinf function for ProcessHandler
def sample_error_handling_function_process(exception: Exception, arguments: Any, traceback: str = None)
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


# Error handling function for PoolHandler
def sample_error_handling_function_pool(exception: Exception, arguments: Any)
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
```

##### ProcessHandler
The process handler keeps track of the last time a process was active. Assuming you are utilizing your system to it's 
full capabilities, it allows you to detect a process that froze for some reason and restart it.
See: `check_process_timeout` and `restart_timed_out_children` functions. It should be noted that arguments, that are 
being processed by the process that froze are going to be lost.

The process handler allows you to check up on the child processes and look at their exit codes as well as running state.
See: `check_processes`

###### PoolHandlers
Since the pool handlers have exceptions, results on a task level, the library _specifically_ doesn't allow you to 
to simply restart processes that have timed out. The programmer should make sure the arguments are valid.
As a result, the process of restarting a given task is a bit more involved and looks something like this:

```python
from src.qdmp import ProcessPoolHandler
from typing import Any


def target_function(arguments: Any) -> Any:
  """
  Sample target function to be executed by the handler. Take this as inspiration.

  :param arguments: Arguments to be processed.
  :return: Results of the computation.
  """
  return arguments ** 2


# Create a handler with 4 processes
handler = ProcessPoolHandler(target_function)
handler.start(4)
handler.add_tasks([1, 2, 3, 4, 5, 6, 7, 8, 9, "y"])
results, errors = handler.process_results()

# Check if any processes have timed out
timed_out_futures = handler.check_process_timeout(timeout=5)

# Cancel the futures
handler.cancel_processes(futures=timed_out_futures)

# Resubmit the futures
handler.add_tasks(futures=[task.arguments for task in timed_out_futures])

handler.stop()
```
Also the code above nicely demonstrates the reason why this is generally a bad idea. `"y"` has no `__pow__` method and
will raise a `TypeError` every time. As a result mindlessly rescheduling a timed out task like this will only waste time
and it is for that reasons that no restart function is provided out of the box.