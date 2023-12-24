class CustomSubprocessError(Exception):
    truncate_cap = 750

    def __init__(self, full_cmd, stdout, stderr):
        from locale import getpreferredencoding

        DEFAULT_ENCODING = getpreferredencoding() or "UTF-8"
        self.full_cmd = full_cmd
        self.stdout = stdout
        self.stderr = stderr

        if self.stdout is None:
            exc_stdout = b"<redirected>"
        else:
            exc_stdout = self.stdout[: self.truncate_cap]
            out_delta = len(self.stdout) - len(exc_stdout)
            if out_delta:
                exc_stdout += (
                    "... (%d more, please see e.stdout)" % out_delta
                ).encode()

        if self.stderr is None:
            exc_stderr = b"<redirected>"
        else:
            exc_stderr = self.stderr[: self.truncate_cap]
            err_delta = len(self.stderr) - len(exc_stderr)
            if err_delta:
                exc_stderr += (
                    "... (%d more, please see e.stderr)" % err_delta
                ).encode()
        msg = "\n\n  RAN: %r\n\n  STDOUT:\n%s\n\n  STDERR:\n%s" % (
            full_cmd,
            exc_stdout.decode(DEFAULT_ENCODING, "replace"),
            exc_stderr.decode(DEFAULT_ENCODING, "replace"),
        )
        super(CustomSubprocessError, self).__init__(msg)
