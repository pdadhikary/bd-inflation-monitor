from .logging import setup_logging


def datapull():
    from .datapull import main

    return main()


def deletedb():
    from .deletedb import main

    return main()


def initdb():
    from .initdb import main

    return main()


def updatedb():
    from .updatedb import main

    return main()


__all__ = ["setup_logging", "initdb", "updatedb", "deletedb", "datapull"]
