import gzip
import os
import subprocess
from timeit import default_timer

from lib.exceptions import CustomSubprocessError


def call_subprocess(command: str, params: list, outfile=None, chdir=None):
    # When we want to pipe the result to a text file, then we have to use the outfile option.
    # If the program asks you to specify the output with -o etc. then leave the outfile param None
    if outfile:
        stdout_buffer = open(outfile, "wb", buffering=0)
    else:
        stdout_buffer = subprocess.PIPE

    popen_args = dict(
        args=[command] + params,
        preexec_fn=os.setsid,
        stdin=subprocess.DEVNULL,
        stdout=stdout_buffer,
        stderr=subprocess.PIPE,
        bufsize=0,
        cwd=chdir,
    )
    process = subprocess.Popen(**popen_args)
    stdout, stderr = process.communicate()

    return_code = process.returncode

    if return_code != 0:
        full_command = " ".join(popen_args["args"])
        raise CustomSubprocessError(full_command, stdout, stderr)
    retstdout = stdout.decode() if stdout is not None else None
    return return_code, retstdout


def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File {file_path} has been deleted.")
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    except OSError as e:
        print(f"Error deleting file {file_path}: {e}")


def is_gzipped(file):
    return str(file).endswith(".gz") or str(file).endswith(".bgz")


def open_func(
    file, read_header=False, header_start="#", skip_rows=0, to_list=False, sep="\t"
):
    """for row count: headers or lines with empty spaces are counted"""
    gzipped = is_gzipped(file)
    file_open = gzip.open if gzipped else open
    row_count = 0
    with file_open(file) as infile:
        for xline in infile:
            row_count += 1
            line = (
                xline.decode("utf-8").replace("\n", "")
                if gzipped
                else xline.replace("\n", "")
            )
            if (
                line == ""
                or (line.startswith(header_start) and read_header is False)
                or row_count <= skip_rows
            ):
                continue
            yield line if not to_list else line.split(sep)


def parse_json_lines(infile):
    import json

    for line in open_func(infile):
        yield json.loads(line)


def load_json(json_file, encoding="utf-8"):
    import json

    json_file = str(json_file)
    if is_gzipped(json_file):
        with gzip.open(json_file) as fh:
            return json.loads(fh.read().decode(encoding=encoding))
    else:
        with open(json_file) as fh:
            return json.load(fh)


def log_message(
    msg,
    initial_time=None,
    debug=True,
    warning=False,
    exception=False,
    print_msg=False,
    tag=None,
):
    import logging

    # import logging.handlers
    from settings import HPO_TO_GENESET_LOG

    logging.basicConfig(
        filename=HPO_TO_GENESET_LOG,
        format="%(asctime)s - %(levelname)s: %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    label = "Hpo to Geneset"
    if tag is not None:
        label = "Hpo to Geneset " + tag
    import os

    if initial_time is not None:
        secs = default_timer() - initial_time
        minutes = secs / 60
        msg = f"Log from {label}:--- {msg} == {secs:.1f} secs, {minutes:.1f} mins"
    if print_msg or os.getenv("PRINT_LOG"):
        print(msg)
    if debug:
        logger.info(msg)
    if warning:
        logger.warning(msg)
    if exception:
        logger.error(msg)


def dump_json(document, outfile, indent=None, encoding="utf-8"):
    import json

    outfile = str(outfile)
    if is_gzipped(outfile):
        with gzip.open(outfile, "w") as fw:
            fw.write(json.dumps(document, indent=indent).encode(encoding=encoding))
    else:
        with open(outfile, "w") as fw:
            json.dump(document, fw, indent=indent)


def get_chunk_from_iterator(iterator, n, to_list=False):
    """
    Iterate an iterator by chunks (of n)
    if with_cnt is True, return (chunk, cnt) each time
    ref http://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    """
    import itertools

    iterator = iter(iterator)
    while True:
        if to_list:
            chunk = list(itertools.islice(iterator, n))
        else:
            chunk = tuple(itertools.islice(iterator, n))
        if not chunk:
            return
        yield chunk
