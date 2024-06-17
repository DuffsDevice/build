# https://stackoverflow.com/a/57084403
from typing import List
import sys
import io
import queue
import threading
import psutil
import subprocess
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

class ProcessResult:
    def __init__(self, stdout, stderr, stdout_lines, stderr_lines, returncode, duration):
        self.stdout = stdout
        self.stderr = stderr
        self.stdout_lines = stdout_lines
        self.stderr_lines = stderr_lines
        self.returncode = returncode
        self.duration = duration

def run_subprocess(
    cmd: str|List[str]
    , stdout  # :multiprocessing.Queue|queue.Queue
    , stderr  # :multiprocessing.Queue|queue.Queue
    , return_code  # :multiprocessing.Queue|queue.Queue
    , arguments: dict
    , timeout_in_seconds: float|None = None
    , print_prefix: str|None = None
) -> int:
    with subprocess.Popen(
        cmd
        , stdout=subprocess.PIPE
        , stderr=subprocess.PIPE
        , text=True
        , **arguments
    ) as process:
        def _enqueue_output(file: io.TextIOWrapper, queue: queue.Queue):  # pylint: disable=redefined-outer-name
            for line in iter(file.readline, ''):
                if line[-1] == "\n":  # remove trailling newline
                    line = line[:-1]
                queue.put(line)
            file.close()
        def _timeout():
            try:
                process.wait(timeout=timeout_in_seconds)
            except subprocess.TimeoutExpired:
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
        if isinstance(print_prefix, str):
            stdout.put(print_prefix.format(process=process))
        with ThreadPoolExecutor(3) as pool:
            stdout_queue, stderr_queue = queue.Queue(), queue.Queue()
            if timeout_in_seconds is not None:
                pool.submit(_timeout)
            pool.submit(_enqueue_output, process.stdout, stdout_queue)
            pool.submit(_enqueue_output, process.stderr, stderr_queue)
            time.sleep(0.01)
            while process.poll() is None or not stdout_queue.empty() or not stderr_queue.empty():
                try:
                    stdout.put(stdout_queue.get_nowait())
                except queue.Empty:
                    pass
                try:
                    stderr.put(stderr_queue.get_nowait())
                except queue.Empty:
                    pass
        return_code.put(process.returncode)

def run(
    cmd: str|List[str]
    , timeout_in_seconds: float|None = None
    , print_stdout: bool|Callable|None = True
    , print_stderr: bool|Callable|None = True
    , print_file: io.TextIOWrapper|None = None
    , separate_process: bool = False
    , print_prefix: str|None = None
    , **arguments
) -> ProcessResult:

    # If print_out or print_err is "True", that means, use the default pipelines
    if print_stdout is True:
        print_stdout = lambda message: print(message, file=sys.stdout, flush=True)
    if print_stderr is True:
        print_stderr = lambda message: print(message, file=sys.stderr, flush=True)

    start_time  = time.time()

    # Determine data structures to pipe data between processes
    if separate_process:
        context         = multiprocessing.get_context('spawn')
        stdout_queue    = context.Queue()
        stderr_queue    = context.Queue()
        return_code     = context.Queue()
        implementation  = context.Process
    else:
        stdout_queue    = queue.Queue()
        stderr_queue    = queue.Queue()
        return_code     = queue.Queue()
        implementation  = threading.Thread

    # Create implementation
    implementation = implementation(
        target=run_subprocess
        , kwargs=dict(
            cmd=cmd
            , stdout=stdout_queue
            , stderr=stderr_queue
            , return_code=return_code
            , arguments=arguments
            , timeout_in_seconds=timeout_in_seconds
            , print_prefix=print_prefix
        )
    )

    # Start the process
    implementation.start()

    # Keep pulling from stdout and stderr queues, until the program finishes
    stdout: List[str] = []
    stderr: List[str] = []
    while implementation.is_alive() or not stdout_queue.empty() or not stderr_queue.empty():
        try:
            line = stdout_queue.get_nowait()
            stdout.append(line)
            if print_stdout:
                print_stdout(line)
            if print_file:
                print(line, flush=True, file=print_file)
        except queue.Empty:
            pass
        try:
            line = stderr_queue.get_nowait()
            stderr.append(line)
            if print_stderr:
                print_stderr(line)
            if print_file:
                print(line, flush=True, file=print_file)
        except queue.Empty:
            pass

    # Return
    return ProcessResult(
        ''.join(stdout)
        , ''.join(stderr)
        , stdout
        , stderr
        , return_code.get()
        , time.time() - start_time
    )
