import glob
import os

from invoke import task
from invoke.exceptions import Exit

from .libs.common.color import color_message


def get_package_path(glob_pattern):
    package_paths = glob.glob(glob_pattern)
    if len(package_paths) > 1:
        raise Exit(code=1, message=color_message(f"Too many files matching {glob_pattern}: {package_paths}", "red"))
    elif len(package_paths) == 0:
        raise Exit(code=1, message=color_message(f"Couldn't find any file matching {glob_pattern}", "red"))

    return package_paths[0]


@task
def compare_size(_, new_package, stable_package, package_type, last_stable, threshold):
    new_package_size = os.path.getsize(get_package_path(new_package))
    stable_package_size = os.path.getsize(get_package_path(stable_package))

    threshold = int(threshold)

    diff = new_package_size - stable_package_size

    if diff > threshold:
        print(
            f"""{package_type} size increase is too large:
  New package size is {new_package_size/1000000:.2f}MB
  Stable package ({last_stable}) size is {stable_package_size/1000000:.2f}MB
  Diff is {diff/1000000:.2f}MB > {threshold/1000000:.2f}MB (max allowed diff)"""
        )
        raise Exit(code=1)

    print(
        f"""{package_type} size increase is OK:
  New package size is {new_package_size/1000000:.2f}MB
  Stable package ({last_stable}) size is {stable_package_size/1000000:.2f}MB
  Diff is {diff/1000000:.2f}MB (max allowed diff: {threshold/1000000:.2f}MB)"""
    )
