# https://stackoverflow.com/a/57084403
from typing import List
import sys, io, queue, psutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

class ProcessResult:
    def __init__(self, stdout, stderr, stdout_lines, stderr_lines, returncode, duration):
        self.stdout = stdout
        self.stderr = stderr
        self.stdout_lines = stdout_lines
        self.stderr_lines = stderr_lines
        self.returncode = returncode
        self.duration = duration

def shell(
    cmd: str|List[str]
    , timeout_in_seconds: float|None = None
    , print_out: bool = True
    , print_file: io.TextIOWrapper|None = None
    , **arguments
) -> ProcessResult:
    def _read_popen_pipes(process: subprocess.Popen, timeout_in_seconds: float|None = None):
        def _enqueue_output(file: io.TextIOWrapper, q: queue.Queue):
            for line in iter(file.readline, ''):
                q.put(line)
            file.close()
        def _timeout():
            try:
                process.wait(timeout=timeout_in_seconds)
            except subprocess.TimeoutExpired:
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
        with ThreadPoolExecutor(3) as pool:
            q_stdout, q_stderr = queue.Queue(), queue.Queue()
            if timeout_in_seconds is not None:
                pool.submit(_timeout)
            pool.submit(_enqueue_output, process.stdout, q_stdout)
            pool.submit(_enqueue_output, process.stderr, q_stderr)
            time.sleep(0.01)
            while process.poll() is None or not q_stdout.empty() or not q_stderr.empty():
                out_line = err_line = ''
                try:
                    out_line = q_stdout.get_nowait()
                except queue.Empty:
                    pass
                try:
                    err_line = q_stderr.get_nowait()
                except queue.Empty:
                    pass
                yield (out_line, err_line)

    start_time = time.time()
    with subprocess.Popen(
        cmd
        , stdout=subprocess.PIPE
        , stderr=subprocess.PIPE
        , text=True
        , **arguments
    ) as process:
        stdout: List[str] = []
        stderr: List[str] = []
        for out_line, err_line in _read_popen_pipes(process, timeout_in_seconds):
            stdout.append(out_line)
            stderr.append(err_line)
            if print_out:
                print(out_line, end='', flush=True)
                print(err_line, end='', file=sys.stderr, flush=True)
            if print_file:
                print(out_line, end='', flush=True, file=print_file)
                print(err_line, end='', flush=True, file=print_file)
        return ProcessResult(
            ''.join(stdout)
            , ''.join(stderr)
            , stdout
            , stderr
            , process.returncode
            , time.time() - start_time
        )
