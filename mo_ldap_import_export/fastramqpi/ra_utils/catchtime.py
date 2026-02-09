# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from contextlib import contextmanager
from time import perf_counter
from time import process_time
from typing import Callable
from typing import Iterator
from typing import Tuple
from typing import Union


@contextmanager
def catchtime(
    include_process_time: bool = False,
) -> Iterator[Union[Callable[[], float], Callable[[], Tuple[float, float]]]]:
    """Measure time spent within the contextmanager.

    Example:
        ```Python
        with catchtime() as t:
            time.sleep(1)
        time_spent = t()
        print(time_spent)  # --> Prints 1.0

        with catchtime(True) as t:
            time.sleep(1)
        time_spent, process_time = t()
        print(time_spent)  # --> Prints 1.0
        print(process_time)  # --> Prints 0.0, as no CPU time has elapsed
        ```

    Args:
        include_process_time:
            Whether to include process-time or solely real-time in result.

    Returns:
        A callable takes no arguments and return either:

        * a single float: realtime, or
        * a tuple (real-time, process-time)

        Depending on the provided `include_process_time` argument.
    """
    real_start = perf_counter()
    process_start = process_time()

    def result_func() -> Tuple[float, float]:
        return (perf_counter() - real_start, process_time() - process_start)

    if include_process_time:
        yield result_func
    else:
        yield lambda: result_func()[0]
