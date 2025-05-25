import re
import platform as pf
import importlib.metadata as md


def troubleshooting_info():
    """Print system, package and deps information relevant to troubleshooting"""
    
    print("OS:", pf.platform())
    python = pf.python_implementation(), pf.python_version()
    print("nfl_data_py", md.version("nfl_data_py"), "on", *python)

    for dep in md.requires("nfl_data_py"):  # type: ignore
        match = re.match(r'^([a-zA-Z_-]+)(.*)$', dep)
        name, constraints = match.groups() if match else (dep, "")
        version = f"v{md.version(name).ljust(12)}"
        print(f" - {name}\t{version}({constraints.strip('=')})")


if __name__ == "__main__":
    troubleshooting_info()