# Vendored-port addition for the Aethonan example (see NOTICE).
#
# Install the robyn-aethonan extension module over the Aethonan package.
#
# The built shared object exports both `PyInit_robyn` (for `robyn.robyn`) and
# `PyInit__aethonan` (for `aethonan._aethonan`). Installing it *over* the
# Aethonan package as `_aethonan.so` means a single shared object backs both
# module names, so the native route registry (and every other process-global
# of the embedded Aethonan rlib) is shared between the ported Robyn core and
# Aethonan's own server classes. The copy that pip installed inside the `robyn`
# package is removed: `robyn/robyn.py` is a pure-Python shim re-exporting the
# module from `aethonan._aethonan`.
#
# The replacement is done with an atomic rename: never overwrite a loaded
# `.so` in place (the running interpreter would SIGBUS).

import pathlib
import shutil
import sys


def main() -> None:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <path to the built extension module>")
        raise SystemExit(1)

    source = pathlib.Path(sys.argv[1]).resolve()
    if not source.is_file():
        print(f"{source} does not exist")
        raise SystemExit(1)

    import aethonan

    aethonan_pkg = pathlib.Path(aethonan.__path__[0])
    for existing in aethonan_pkg.glob("_aethonan*.so"):
        existing.unlink()
    target = aethonan_pkg / "_aethonan.so"
    tmp = aethonan_pkg / "._aethonan.so.tmp"
    shutil.copy(source, tmp)
    os_replace = getattr(__import__("os"), "replace")
    os_replace(tmp, target)
    print(f"installed {target}")

    # Drop the second copy pip installed inside the robyn package, so that
    # `import robyn.robyn` resolves to the shim and both module names share
    # the single shared object installed above. Scan sys.path instead of
    # importing robyn: an import from the current working directory (e.g. the
    # example checkout, whose robyn package has no .so) would shadow the
    # installed package and leave the duplicate copy in place.
    cwd = pathlib.Path.cwd().resolve()
    for entry in sys.path:
        if not entry:
            continue
        root = pathlib.Path(entry).resolve()
        if root == cwd:
            continue
        robyn_pkg = root / "robyn"
        if robyn_pkg.is_dir():
            for installed in robyn_pkg.glob("robyn*.so"):
                installed.unlink()
                print(f"removed {installed} (robyn.robyn is a shim over aethonan._aethonan)")


if __name__ == "__main__":
    main()
