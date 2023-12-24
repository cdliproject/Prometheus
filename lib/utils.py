import gzip
import json
import logging
import os
import subprocess
from itertools import islice
from timeit import default_timer
from typing import Any, Iterable, Iterator, List, Tuple, Union

from lib.exceptions import CustomSubprocessError


def call_subprocess(
    command: str,
    params: list,
    outfile: Union[None, str] = None,
    chdir: Union[None, str] = None,
) -> Tuple[int, Union[None, str]]:
    stdout_option = subprocess.PIPE
    file_handle = None

    if outfile:
        file_handle = open(outfile, "wb", buffering=0)
        stdout_option = file_handle

    popen_args = {
        "args": [command] + params,
        "preexec_fn": os.setsid,
        "stdin": subprocess.DEVNULL,
        "stdout": stdout_option,
        "stderr": subprocess.PIPE,
        "bufsize": 0,
        "cwd": chdir,
    }

    try:
        with subprocess.Popen(**popen_args) as process:
            stdout, stderr = process.communicate()
            return_code = process.returncode

        if return_code != 0:
            error_msg = f"Command '{' '.join(popen_args['args'])}' failed with return code {return_code}"
            raise CustomSubprocessError(error_msg, stdout, stderr)

        return return_code, stdout.decode() if stdout else None

    finally:
        if file_handle:
            file_handle.close()


def delete_file(file_path: str) -> bool:
    """
    Attempts to delete a file.

    :param file_path: Path to the file to be deleted.
    :return: True if file was successfully deleted, False otherwise.
    """
    try:
        os.remove(file_path)
        print(f"File {file_path} has been deleted.")
        return True
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    except OSError as e:
        print(f"Error deleting file {file_path}: {e}")
    return False


def is_gzipped(file_path: str) -> bool:
    """
    Checks if a file is gzipped, first by its extension and then by its magic number.

    :param file_path: Path to the file to be checked.
    :return: True if the file is gzipped, False otherwise.
    """
    if not file_path.endswith((".gz", ".bgz")):
        return False

    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return False

    try:
        with open(file_path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"  # Gzip magic number
    except OSError as e:
        print(f"Error reading file {file_path}: {e}")
        return False


def open_func(
    file: str,
    read_header: bool = False,
    header_start: str = "#",
    skip_rows: int = 0,
    to_list: bool = False,
    sep: str = "\t",
    encoding: str = "utf-8",
) -> Iterator:
    """
    Opens a file and iterates over its lines with optional parsing.

    :param file: Path to the file.
    :param read_header: Whether to read header lines.
    :param header_start: The character that denotes header lines.
    :param skip_rows: Number of initial rows to skip.
    :param to_list: Whether to split lines into lists based on 'sep'.
    :param sep: Separator for splitting lines.
    :param encoding: File encoding.
    :return: An iterator over the lines of the file.
    """
    file_open = gzip.open if is_gzipped(file) else open
    try:
        with file_open(file, "rt", encoding=encoding) as infile:
            for line_number, line in enumerate(infile, start=1):
                line = line.strip()
                if (
                    not line
                    or (line.startswith(header_start) and not read_header)
                    or line_number <= skip_rows
                ):
                    continue
                yield line.split(sep) if to_list else line
    except Exception as e:
        print(f"Error opening file {file}: {e}")


def parse_json_lines(infile: str) -> Iterator:
    """
    Parses a file containing JSON lines.

    :param infile: Path to the input file.
    :return: An iterator over the parsed JSON objects.
    """
    for line in open_func(infile):
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON line: {e}")


def load_json(json_file: str) -> dict:
    """
    Loads a JSON file.

    :param json_file: Path to the JSON file.
    :return: The loaded JSON object.
    """
    with gzip.open(json_file, "rt") if is_gzipped(json_file) else open(
        json_file, "r"
    ) as fh:
        try:
            return json.load(fh)
        except json.JSONDecodeError as e:
            print(f"Error loading JSON file {json_file}: {e}")
            return {}


def log_message(
    msg: str,
    log_file: str,
    initial_time: Union[None, float] = None,
    debug: bool = True,
    warning: bool = False,
    exception: bool = False,
    print_msg: bool = False,
    tag: Union[None, str] = None,
) -> None:
    """
    Logs a message to a specified log file with various options.

    :param msg: The message to log.
    :param log_file: Path to the log file.
    :param initial_time: Initial time for time elapsed calculation.
    :param debug: Flag to indicate if this is a debug message.
    :param warning: Flag to indicate if this is a warning message.
    :param exception: Flag to indicate if this is an exception message.
    :param print_msg: Flag to indicate if the message should also be printed to the console.
    :param tag: Optional tag to prepend to the message.
    """
    logging.basicConfig(
        filename=log_file,
        format="%(asctime)s - %(levelname)s: %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    label = f"Hpo to Geneset {tag}" if tag else "Hpo to Geneset"

    if initial_time is not None:
        elapsed_time = default_timer() - initial_time
        msg = f"Log from {label}:--- {msg} == {elapsed_time:.1f} secs, {elapsed_time / 60:.1f} mins"

    if print_msg:
        print(msg)

    log_function = logger.info if debug else logger.warning if warning else logger.error
    log_function(msg)


def dump_json(
    document: Any, outfile: str, indent: int = None, gzip_compression_level: int = 9
) -> None:
    """
    Dumps a JSON document to a file, optionally gzipped.

    :param document: The JSON document to dump.
    :param outfile: Path to the output file.
    :param indent: Indentation level for pretty-printing.
    :param gzip_compression_level: Gzip compression level (if applicable).
    """
    mode = "wt"
    open_func = (
        lambda f: gzip.open(f, mode, compresslevel=gzip_compression_level)
        if is_gzipped(outfile)
        else open(f, mode)
    )
    try:
        with open_func(outfile) as fw:
            json.dump(document, fw, indent=indent, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing to file {outfile}: {e}")


def get_chunk_from_iterator(
    iterator: Iterable, chunk_size: int, to_list: bool = False
) -> Iterator[Union[Tuple, List]]:
    """
    Yield chunks of size 'chunk_size' from 'iterator'.

    Args:
        iterator (Iterable): The iterator to chunk.
        chunk_size (int): The size of each chunk.
        to_list (bool): If True, yield chunks as lists; otherwise, as tuples.

    Yields:
        Union[Tuple, List]: A chunk of the iterator.
    """
    iterator = iter(iterator)
    for chunk in iter(lambda: list(islice(iterator, chunk_size)), []):
        yield chunk if not to_list else list(chunk)
