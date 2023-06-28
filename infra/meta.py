def upgrade_packages():
    """Upgrade all packages in the current virtual environment."""
    import pkg_resources
    from subprocess import call

    for dist in pkg_resources.working_set:
        call("python -m pip install --upgrade " + dist.project_name, shell=True)
        
if __name__ == "__main__":
    upgrade_packages()