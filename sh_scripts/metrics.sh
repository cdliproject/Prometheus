radon cc filter.py -s # Calculate cyclomatic complexity
echo "\n------------------------\n"
radon hal filter.py # Calculate halstrad measures 
echo "\n------------------------\n"
radon mi filter.py -s # Calculate maintainability index | A high value means better maintainability
echo "\n------------------------\n"



# Radon is a py tool computes various code metrics. Supported metrics are:
    # Raw metrics: SLOC, comment lines, blank lines
    # Cyclomatic Complexity => number of decisions a block of code contains
    # Halstead metrics
    # Maintainability Index => measures how maintainable (easy to support and change) your source code is.
        # The original formula ==>  MI=171−5.2lnV−0.23G−16.2lnL
        # V = Halstead Volume
        # G = Halstead Difficulty
        # L = Lines of Code


