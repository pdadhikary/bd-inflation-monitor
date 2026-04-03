from .logging import setup_logging


def datapull():
    from .scripts.datapull import main

    return main()


def deletedb():
    from .scripts.deletedb import main

    return main()


def initdb():
    from .scripts.initdb import main

    return main()


def updatedb():
    from .scripts.updatedb import main

    return main()


def dbmigrate():
    from .scripts.dbmigrate import main

    return main()


__all__ = ["setup_logging", "initdb", "updatedb", "deletedb", "datapull", "dbmigrate"]
