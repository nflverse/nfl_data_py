import platform as pf
import importlib.metadata as md


def troubleshooting_info():
    """Print system, package and deps information relevant to troubleshooting"""
    
    print("nfl_data_py", md.version("nfl_data_py"))
    print(pf.python_implementation(), pf.python_version())
    print(pf.platform())
    print("Dependencies:")
    for pkg in md.requires("nfl_data_py"):
        name, constraints = pkg.split(maxsplit=1)
        version = f"v{md.version(name).strip('.0')}"
        print(f" - {name}\t{version}\t({constraints})")


if __name__ == "__main__":
    troubleshooting_info()