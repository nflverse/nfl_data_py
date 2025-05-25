import platform as pf
import importlib.metadata as md

from nfl_data_py import __version__

def troubleshooting_info():
    """Print system, package and deps information relevant to troubleshooting"""
    
    print("nfl_data_py", __version__)
    print(pf.python_implementation(), pf.python_version())
    print(pf.platform())

    dependencies = md.requires("nfl_data_py")
    assert isinstance(dependencies, list)

    print("Dependencies:")
    for pkg in dependencies:
        print(pkg)
        name, constraints = pkg.split(maxsplit=1)
        version = f"v{md.version(name).strip('.0')}"
        print(f" - {name}\t{version}\t({constraints})")


if __name__ == "__main__":
    troubleshooting_info()